# markets/bitfinex.py
import asyncio, aiohttp, websockets, json, time
from typing import List, Set, Dict, Optional, Callable, Tuple

# ---------- Quote-compatible object (same fields main expects) ----------
class _QuoteCompat:
    __slots__ = ("bid","ask","bid_sz","ask_sz","bid_str","ask_str","ts_ms")
    def __init__(self, bid=None, ask=None, bid_sz=None, ask_sz=None, bid_str=None, ask_str=None, ts_ms=0):
        self.bid, self.ask = bid, ask
        self.bid_sz, self.ask_sz = bid_sz, ask_sz
        self.bid_str, self.ask_str = bid_str, ask_str
        self.ts_ms = ts_ms

def now_ms() -> int:
    return int(time.time() * 1000)

def chunked(seq, n):
    return [seq[i:i+n] for i in range(0, len(seq), n)]

# Bitfinex quote code mapping
QUOTE_TO_BFX = {"USDT": "UST", "USDC": "UDC", "USD": "USD", "EUR": "EUR", "BTC": "BTC"}
BFX_TO_QUOTE = {v: k for k, v in QUOTE_TO_BFX.items()}

def _human_from_bfx(code: str) -> Optional[str]:
    # code example: "BTCUSD", "ETHUST", "LTCUDC"
    # find suffix among known BFX quote codes
    for suf in BFX_TO_QUOTE.keys():
        if code.endswith(suf):
            base = code[:-len(suf)]
            quote = BFX_TO_QUOTE[suf]
            return f"{base}/{quote}"
    return None

def _bfx_symbol_from_pair(pair: str) -> str:
    # "ETH/USDT" -> "tETHUST"
    base, quote = pair.split("/")
    q = QUOTE_TO_BFX.get(quote.upper(), quote.upper())
    return f"t{base.upper()}{q}"

class BitfinexMarket:
    name = "bitfinex"
    on_quote: Optional[Callable[[str, _QuoteCompat], None]] = None

    # Tunables
    SUB_BATCH = 35
    MAX_SIZE = 2**22
    PING_INTERVAL = 20
    PING_TIMEOUT = 20
    BFX_BOOK_PREC = "P0"     # P0..P3
    BFX_BOOK_FREQ = "F0"     # F0 (realtime) or F1 (~2s)
    BFX_BOOK_LEN  = 25       # 1,25,100
    REST_CONF = "https://api-pub.bitfinex.com/v2/conf/pub:list:pair:exchange"
    WS_URL = "wss://api-pub.bitfinex.com/ws/2"

    # ---------- Discovery ----------
    async def discover(self, desired_pairs: List[str]) -> Set[str]:
        """
        Return subset of desired_pairs supported on Bitfinex spot; cache pair->symbol.
        """
        ok: Set[str] = set()
        mapping: Dict[str, str] = {}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.REST_CONF, timeout=20) as r:
                    j = await r.json()
            # response like: [["BTCUSD","ETHUST",...]]
            if not (isinstance(j, list) and j and isinstance(j[0], list)):
                raise ValueError("Bad Bitfinex conf response")
            all_codes = set(j[0])
            desired = set(desired_pairs)
            for code in all_codes:
                human = _human_from_bfx(code)
                if human and human in desired:
                    ok.add(human)
                    mapping[human] = f"t{code}"
        except Exception as e:
            print("[bitfinex][discover] error:", e)

        self._pair_to_bfxsym = mapping
        return ok

    # ---------- Consumer ----------
    async def _consume(self, batch: List[str]):
        # books[pair] = {"bids": {price: (count, amount)}, "asks": {...}}
        books: Dict[str, Dict[str, Dict[float, Tuple[int, float]]]] = {}

        while True:
            try:
                async with websockets.connect(
                    self.WS_URL,
                    ping_interval=self.PING_INTERVAL,
                    ping_timeout=self.PING_TIMEOUT,
                    max_size=self.MAX_SIZE,
                ) as ws:
                    chan_to_pair: Dict[int, str] = {}
                    # subscribe per symbol
                    for p in batch:
                        sym = self._pair_to_bfxsym.get(p)
                        if not sym:
                            continue
                        msg = {
                            "event": "subscribe",
                            "channel": "book",
                            "symbol": sym,
                            "prec": self.BFX_BOOK_PREC,
                            "freq": self.BFX_BOOK_FREQ,
                            "len": self.BFX_BOOK_LEN,
                        }
                        await ws.send(json.dumps(msg))
                        books[p] = {"bids": {}, "asks": {}}

                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue

                        # subscription ack
                        if isinstance(msg, dict):
                            if msg.get("event") == "subscribed" and msg.get("channel") == "book":
                                sym = msg.get("symbol")
                                pair = next((p for p in batch if self._pair_to_bfxsym.get(p) == sym), None)
                                if pair:
                                    chan_to_pair[int(msg["chanId"])] = pair
                            # ignore other events
                            continue

                        # data messages are arrays: [chanId, payload]
                        if not (isinstance(msg, list) and len(msg) >= 2):
                            continue
                        chan_id, payload = msg[0], msg[1]
                        pair = chan_to_pair.get(int(chan_id))
                        if not pair:
                            continue
                        if payload == "hb":
                            continue

                        # snapshot (list of lists) or single update (3-tuple)
                        if isinstance(payload, list) and payload and isinstance(payload[0], list):
                            # snapshot
                            bids, asks = {}, {}
                            for entry in payload:
                                try:
                                    price = float(entry[0]); count = int(entry[1]); amount = float(entry[2])
                                except:
                                    continue
                                if count == 0:
                                    continue
                                if amount > 0:
                                    bids[price] = (count, amount)
                                elif amount < 0:
                                    asks[price] = (count, amount)
                            books[pair]["bids"], books[pair]["asks"] = bids, asks

                        elif isinstance(payload, list) and len(payload) == 3:
                            # single level update
                            try:
                                price = float(payload[0]); count = int(payload[1]); amount = float(payload[2])
                            except:
                                continue
                            side = "bids" if amount > 0 else "asks"
                            if count > 0:
                                books[pair][side][price] = (count, amount)
                            else:
                                books[pair][side].pop(price, None)
                        else:
                            continue

                        # derive top-of-book
                        bids = books[pair]["bids"]; asks = books[pair]["asks"]
                        best_bid = max(bids.keys()) if bids else None
                        best_ask = min(asks.keys()) if asks else None
                        if best_bid is None and best_ask is None:
                            continue

                        bid_sz = abs(bids[best_bid][1]) if best_bid is not None else None
                        ask_sz = abs(asks[best_ask][1]) if best_ask is not None else None

                        # keep raw strings for full precision printing
                        bid_str = f"{best_bid:.12f}" if best_bid is not None else None
                        ask_str = f"{best_ask:.12f}" if best_ask is not None else None

                        q = _QuoteCompat(
                            bid=best_bid, ask=best_ask,
                            bid_sz=bid_sz, ask_sz=ask_sz,
                            bid_str=bid_str, ask_str=ask_str,
                            ts_ms=now_ms(),
                        )
                        if self.on_quote:
                            self.on_quote(pair, q)

            except Exception as e:
                print("[bitfinex] reconnecting after error:", e)
                await asyncio.sleep(3)

    # ---------- Runner ----------
    async def run(self, pairs: List[str]) -> None:
        # ensure mapping present even if discover() was skipped
        self._pair_to_bfxsym = getattr(self, "_pair_to_bfxsym", {})
        for p in pairs:
            if p not in self._pair_to_bfxsym:
                self._pair_to_bfxsym[p] = _bfx_symbol_from_pair(p)

        if not pairs:
            return
        batches = chunked(sorted(pairs), self.SUB_BATCH)
        await asyncio.gather(*(self._consume(b) for b in batches))

# Entry point factory for main.py
def MARKET_CLASS():
    return BitfinexMarket()
