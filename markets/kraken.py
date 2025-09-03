# markets/kraken.py
import asyncio, aiohttp, websockets, json, time
from typing import List, Set, Dict, Optional, Callable

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

# Kraken uses XBT for BTC in wsname; normalize
_WS_TO_HUMAN = {"XBT": "BTC"}
_HUMAN_TO_WS = {v: k for k, v in _WS_TO_HUMAN.items()}

def _wsname_to_human(wsname: str) -> str:
    try:
        b, q = wsname.split("/")
        return f"{_WS_TO_HUMAN.get(b, b)}/{_WS_TO_HUMAN.get(q, q)}"
    except:
        return wsname

def _human_to_wsname(pair: str) -> str:
    b, q = pair.split("/")
    return f"{_HUMAN_TO_WS.get(b, b)}/{_HUMAN_TO_WS.get(q, q)}"

class KrakenMarket:
    name = "kraken"
    on_quote: Optional[Callable[[str, _QuoteCompat], None]] = None

    # Tunables
    SUB_BATCH = 60
    MAX_SIZE = 2**22
    PING_INTERVAL = 20
    PING_TIMEOUT = 20
    KRAKEN_BOOK_DEPTH = 10  # can be 10, 25, etc.
    REST_ASSET_PAIRS = "https://api.kraken.com/0/public/AssetPairs"
    WS_URL = "wss://ws.kraken.com/"

    # ---------- Discovery ----------
    async def discover(self, desired_pairs: List[str]) -> Set[str]:
        """
        Return subset of desired_pairs supported on Kraken Spot, and cache pair->wsname.
        """
        ok: Set[str] = set()
        mapping: Dict[str, str] = {}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.REST_ASSET_PAIRS, timeout=25) as r:
                    j = await r.json()
            result = j.get("result", {}) or {}
            desired = set(desired_pairs)
            for _k, info in result.items():
                wsname = info.get("wsname")
                if not wsname:
                    continue
                human = _wsname_to_human(wsname)
                if human in desired:
                    ok.add(human)
                    mapping[human] = wsname
        except Exception as e:
            print("[kraken][discover] error:", e)

        self._pair_to_wsname = mapping
        return ok

    # ---------- Consumer ----------
    async def _consume(self, batch: List[str]):
        # books[pair] = {"bids": {price: volume}, "asks": {price: volume}}
        books: Dict[str, Dict[str, Dict[float, float]]] = {}
        while True:
            try:
                async with websockets.connect(
                    self.WS_URL,
                    ping_interval=self.PING_INTERVAL,
                    ping_timeout=self.PING_TIMEOUT,
                    max_size=self.MAX_SIZE,
                ) as ws:
                    chan_to_pair: Dict[int, str] = {}
                    # prepare subscription list
                    pair_list = []
                    for p in batch:
                        wsname = self._pair_to_wsname.get(p)
                        if wsname:
                            pair_list.append(wsname)
                            books[p] = {"bids": {}, "asks": {}}
                    if not pair_list:
                        return

                    sub = {
                        "event": "subscribe",
                        "pair": pair_list,
                        "subscription": {"name": "book", "depth": self.KRAKEN_BOOK_DEPTH},
                    }
                    await ws.send(json.dumps(sub))

                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue

                        # Control / status messages
                        if isinstance(msg, dict):
                            if msg.get("event") == "subscriptionStatus" and msg.get("status") == "subscribed" and \
                               (msg.get("channelName") or "").startswith("book"):
                                cid = msg.get("channelID")
                                wsname = msg.get("pair")
                                human = _wsname_to_human(wsname) if wsname else None
                                if cid is not None and human in batch:
                                    chan_to_pair[int(cid)] = human
                            # ignore heartbeats and others
                            continue

                        # Data messages are lists: [channelID, payload, pair] (sometimes with extra fields)
                        if not (isinstance(msg, list) and len(msg) >= 2):
                            continue

                        cid = msg[0]
                        payload = msg[1]
                        pair = chan_to_pair.get(int(cid))
                        if not pair:
                            continue
                        if payload == "heartbeat":
                            continue
                        if not isinstance(payload, dict):
                            continue

                        # Snapshot
                        if "as" in payload or "bs" in payload:
                            bids, asks = {}, {}
                            for key, target in (("bs", bids), ("as", asks)):
                                for lvl in payload.get(key, []):
                                    try:
                                        price_str = lvl[0]; vol_str = lvl[1]
                                        price = float(price_str); vol = float(vol_str)
                                    except:
                                        continue
                                    if vol > 0:
                                        target[price] = vol
                            books[pair]["bids"], books[pair]["asks"] = bids, asks

                        # Updates
                        if "a" in payload or "b" in payload:
                            for key, name in (("b", "bids"), ("a", "asks")):
                                for lvl in payload.get(key, []):
                                    try:
                                        price_str = lvl[0]; vol_str = lvl[1]
                                        price = float(price_str); vol = float(vol_str)
                                    except:
                                        continue
                                    if vol <= 0:
                                        books[pair][name].pop(price, None)
                                    else:
                                        books[pair][name][price] = vol

                        bids = books[pair]["bids"]; asks = books[pair]["asks"]
                        best_bid = max(bids.keys()) if bids else None
                        best_ask = min(asks.keys()) if asks else None
                        if best_bid is None and best_ask is None:
                            continue

                        bid_sz = bids.get(best_bid) if best_bid is not None else None
                        ask_sz = asks.get(best_ask) if best_ask is not None else None

                        # keep raw strings for full precision display
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
                print("[kraken] reconnecting after error:", e)
                await asyncio.sleep(3)

    # ---------- Runner ----------
    async def run(self, pairs: List[str]) -> None:
        # ensure mapping present even if discover() was skipped
        self._pair_to_wsname = getattr(self, "_pair_to_wsname", {})
        for p in pairs:
            if p not in self._pair_to_wsname:
                self._pair_to_wsname[p] = _human_to_wsname(p)

        if not pairs:
            return
        batches = chunked(sorted(pairs), self.SUB_BATCH)
        await asyncio.gather(*(self._consume(b) for b in batches))

# Entry point factory for main.py
def MARKET_CLASS():
    return KrakenMarket()
