# markets/gate.py
import asyncio, aiohttp, websockets, json, time
from typing import List, Set, Dict, Optional, Callable, Tuple

# ---- Quote-compatible object (same fields main expects) ----
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

def _sym_gate(pair: str) -> str:
    # "ETH/USDT" -> "ETH_USDT"
    return pair.replace("/", "_").upper()

class GateMarket:
    name = "gate"
    on_quote: Optional[Callable[[str, _QuoteCompat], None]] = None

    # Tunables
    SUB_BATCH = 60
    MAX_SIZE = 2**22
    PING_INTERVAL = 20
    PING_TIMEOUT = 20
    WS_URL = "wss://api.gateio.ws/ws/v4/"
    GATE_LEVELS = "5"        # 5,10,20,50,100
    GATE_INTERVAL = "100ms"  # 100ms or 1000ms

    # ========= Discovery =========
    async def discover(self, desired_pairs: List[str]) -> Set[str]:
        """
        Return subset of desired_pairs that are tradable on Gate.io spot.
        """
        url = "https://api.gateio.ws/api/v4/spot/currency_pairs"
        ok: Set[str] = set()
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=20) as r:
                    j = await r.json()
            if not isinstance(j, list):
                return ok
            # Map "ETH_USDT" -> "tradable"/"untradable"
            avail: Dict[str, str] = { (it.get("id") or "").upper(): (it.get("trade_status") or "").lower()
                                      for it in j if isinstance(it, dict) }
            for p in desired_pairs:
                gid = _sym_gate(p)
                if avail.get(gid) == "tradable":
                    ok.add(p)
        except Exception as e:
            print("[gate][discover] error:", e)
        return ok

    # ========= Consumer =========
    async def _consume(self, batch_pairs: List[str]):
        subs = [
            {
                "time": int(time.time()),
                "channel": "spot.order_book",
                "event": "subscribe",
                "payload": [_sym_gate(p), self.GATE_LEVELS, self.GATE_INTERVAL],
            }
            for p in batch_pairs
        ]

        while True:
            try:
                async with websockets.connect(
                    self.WS_URL,
                    ping_interval=self.PING_INTERVAL,
                    ping_timeout=self.PING_TIMEOUT,
                    max_size=self.MAX_SIZE,
                    compression=None,
                ) as ws:
                    # send subs
                    for sub in subs:
                        await ws.send(json.dumps(sub))

                    async for raw in ws:
                        try:
                            data = json.loads(raw)
                        except Exception:
                            continue

                        if data.get("event") != "update":
                            # ignore acks/pongs/etc
                            continue
                        if data.get("channel") != "spot.order_book":
                            continue

                        res = data.get("result") or {}
                        sym = res.get("s") or ""
                        pair = next((p for p in batch_pairs if _sym_gate(p) == sym), None)
                        if not pair:
                            continue

                        bids = res.get("bids") or []
                        asks = res.get("asks") or []

                        # Take top-of-book (Gate returns arrays of [price,size] as strings)
                        bid_px = bid_sz = ask_px = ask_sz = None
                        bid_px_str = bid_sz_str = ask_px_str = ask_sz_str = None

                        if bids and bids[0]:
                            bid_px_str, bid_sz_str = str(bids[0][0]), str(bids[0][1])
                            try:
                                bid_px = float(bid_px_str); bid_sz = float(bid_sz_str)
                            except:
                                bid_px = bid_sz = None
                        if asks and asks[0]:
                            ask_px_str, ask_sz_str = str(asks[0][0]), str(asks[0][1])
                            try:
                                ask_px = float(ask_px_str); ask_sz = float(ask_sz_str)
                            except:
                                ask_px = ask_sz = None

                        if bid_px is None and ask_px is None:
                            continue

                        q = _QuoteCompat(
                            bid=bid_px, ask=ask_px,
                            bid_sz=bid_sz, ask_sz=ask_sz,
                            bid_str=bid_px_str, ask_str=ask_px_str,
                            ts_ms=now_ms()
                        )
                        if self.on_quote:
                            self.on_quote(pair, q)

            except Exception as e:
                print("[gate] reconnecting after error:", e)
                await asyncio.sleep(3)

    # ========= Runner =========
    async def run(self, pairs: List[str]) -> None:
        if not pairs:
            return
        batches = chunked(sorted(pairs), self.SUB_BATCH)
        await asyncio.gather(*(self._consume(b) for b in batches))

# Entry point factory for main.py
def MARKET_CLASS():
    return GateMarket()
