# markets/bitstamp.py
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

def _sym_bs(pair: str) -> str:
    # "BTC/USD" -> "btcusd"
    return pair.replace("/", "").lower()

class BitstampMarket:
    name = "bitstamp"
    on_quote: Optional[Callable[[str, _QuoteCompat], None]] = None

    # Tunables
    SUB_BATCH = 50
    MAX_SIZE = 2**22
    PING_INTERVAL = 20
    PING_TIMEOUT = 20
    REST_PAIRS = "https://www.bitstamp.net/api/v2/trading-pairs-info/"
    WS_URL = "wss://ws.bitstamp.net"

    # ---------- Discovery ----------
    async def discover(self, desired_pairs: List[str]) -> Set[str]:
        """
        Return subset of desired_pairs that exist on Bitstamp (by url_symbol).
        """
        ok: Set[str] = set()
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.REST_PAIRS, timeout=20) as r:
                    arr = await r.json()
            url_syms = set()
            for it in arr if isinstance(arr, list) else []:
                sym = (it.get("url_symbol") or "").lower().strip()
                if sym:
                    url_syms.add(sym)
            for p in desired_pairs:
                if _sym_bs(p) in url_syms:
                    ok.add(p)
        except Exception as e:
            print("[bitstamp][discover] error:", e)
        return ok

    # ---------- Helpers ----------
    @staticmethod
    def _set_level(side: str, book: Dict[str, Dict[str, float]], price: float, size: float):
        levels = book['bids'] if side == "buy" else book['asks']
        k = f"{price:.12f}"
        if size == 0.0:
            levels.pop(k, None)
        else:
            levels[k] = size

    @staticmethod
    def _best_levels(book: Dict[str, Dict[str, float]]) -> Tuple[Optional[Tuple[float,float]], Optional[Tuple[float,float]]]:
        bids = book['bids']; asks = book['asks']
        best_bid = None
        if bids:
            pb = max((float(p) for p,s in bids.items() if s > 0.0), default=None)
            if pb is not None:
                best_bid = (pb, bids[f"{pb:.12f}"])
        best_ask = None
        if asks:
            pa = min((float(p) for p,s in asks.items() if s > 0.0), default=None)
            if pa is not None:
                best_ask = (pa, asks[f"{pa:.12f}"])
        return best_bid, best_ask

    # ---------- Consumer ----------
    async def _consume(self, batch: List[str]):
        symbols = [_sym_bs(p) for p in batch]
        # book store per pair: {"bids": {price_str: size}, "asks": {...}}
        books: Dict[str, Dict[str, Dict[str, float]]] = {p: {"bids": {}, "asks": {}} for p in batch}

        # build all subscribe frames (snapshot + diff per symbol)
        subs = []
        for s in symbols:
            subs.append({"event": "bts:subscribe", "data": {"channel": f"order_book_{s}"}})
            subs.append({"event": "bts:subscribe", "data": {"channel": f"order_book_{s}_diff"}})

        while True:
            try:
                async with websockets.connect(
                    self.WS_URL,
                    ping_interval=self.PING_INTERVAL,
                    ping_timeout=self.PING_TIMEOUT,
                    max_size=self.MAX_SIZE,
                ) as ws:
                    for msg in subs:
                        await ws.send(json.dumps(msg))
                    # print(f"[bitstamp][ws] subscribed {len(symbols)} symbols (snap+diff)")

                    async for raw in ws:
                        try:
                            data = json.loads(raw)
                        except Exception:
                            continue

                        ev = data.get("event")
                        if ev in ("bts:subscription_succeeded", "bts:request_reconnect", "bts:heartbeat"):
                            continue
                        if ev in ("bts:error", "error"):
                            print("[bitstamp][ws][error]:", data); continue
                        if ev != "data":
                            continue

                        ch = (data.get("channel") or "").lower()
                        payload = data.get("data") or {}
                        bids = payload.get("bids") or []
                        asks = payload.get("asks") or []

                        # ch example: "order_book_btcusd" or "..._diff"
                        is_diff = ch.endswith("_diff")
                        base_sym = ch.replace("order_book_", "").replace("_diff", "")
                        pair = next((p for p in batch if _sym_bs(p) == base_sym), None)
                        if not pair:
                            continue
                        book = books[pair]

                        if not is_diff:
                            # full snapshot
                            book['bids'].clear(); book['asks'].clear()
                            for pr, sz in bids:
                                try:
                                    self._set_level("buy", book, float(pr), float(sz))
                                except:
                                    pass
                            for pr, sz in asks:
                                try:
                                    self._set_level("sell", book, float(pr), float(sz))
                                except:
                                    pass
                        else:
                            # incremental updates; 0-size => remove level
                            for pr, sz in bids:
                                try:
                                    price = float(pr); size = float(sz)
                                except:
                                    continue
                                self._set_level("buy", book, price, size)
                            for pr, sz in asks:
                                try:
                                    price = float(pr); size = float(sz)
                                except:
                                    continue
                                self._set_level("sell", book, price, size)

                        # derive top-of-book and emit
                        bb, aa = self._best_levels(book)
                        if not (bb or aa):
                            continue

                        bid_px = bb[0] if bb else None
                        bid_sz = bb[1] if bb else None
                        ask_px = aa[0] if aa else None
                        ask_sz = aa[1] if aa else None

                        bid_str = f"{bid_px:.12f}" if bid_px is not None else None
                        ask_str = f"{ask_px:.12f}" if ask_px is not None else None

                        q = _QuoteCompat(
                            bid=bid_px, ask=ask_px,
                            bid_sz=bid_sz, ask_sz=ask_sz,
                            bid_str=bid_str, ask_str=ask_str,
                            ts_ms=now_ms(),
                        )
                        if self.on_quote:
                            self.on_quote(pair, q)

            except Exception as e:
                print("[bitstamp] reconnecting after error:", e)
                await asyncio.sleep(3)

    # ---------- Runner ----------
    async def run(self, pairs: List[str]) -> None:
        if not pairs:
            return
        batches = chunked(sorted(pairs), self.SUB_BATCH)
        await asyncio.gather(*(self._consume(b) for b in batches))

# Entry point factory for main.py
def MARKET_CLASS():
    return BitstampMarket()
