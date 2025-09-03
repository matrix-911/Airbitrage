# markets/binance.py
import asyncio, aiohttp, websockets, json, time, os
from typing import List, Set, Dict, Optional, Callable

# ===== Public interface expected by main.py =====
# - Exports MARKET_CLASS which main() will instantiate.
# - Instance must have:
#     .name: str
#     .on_quote: Callable[[pair:str, Quote], None]  (set by main)
#     .discover(desired_pairs: List[str]) -> Set[str]
#     .run(pairs: List[str]) -> None
#
# The Quote class is defined in main, but we only need to pass a dict-like payload.
# We'll construct a simple compatible object with the same fields.
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

class BinanceMarket:
    name = "binance"
    on_quote: Optional[Callable[[str, _QuoteCompat], None]] = None

    # Tunables (edit if needed)
    SUB_BATCH = 60
    DEPTH_LEVELS = 5
    DEPTH_INTERVAL = "100ms"  # must match WS interval
    PING_INTERVAL = 20
    PING_TIMEOUT = 20
    MAX_SIZE = 2**22

    def __init__(self):
        pass

    @staticmethod
    def _pair_to_binance(pair: str) -> str:
        return pair.replace("/", "").upper()

    async def discover(self, desired_pairs: List[str]) -> Set[str]:
        """Filter desired_pairs to those tradable on Binance."""
        url = "https://api.binance.com/api/v3/exchangeInfo"
        ok: Set[str] = set()
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=20) as r:
                    j = await r.json()
            for sym in j.get("symbols", []):
                if sym.get("status") != "TRADING":
                    continue
                base = sym.get("baseAsset", "").upper()
                quote = sym.get("quoteAsset", "").upper()
                human = f"{base}/{quote}"
                if human in desired_pairs:
                    ok.add(human)
        except Exception as e:
            print("[binance][discover] error:", e)
        return ok

    async def _consume(self, batch: List[str]):
        streams = "/".join(f"{self._pair_to_binance(p).lower()}@depth{self.DEPTH_LEVELS}@{self.DEPTH_INTERVAL}" for p in batch)
        url = f"wss://stream.binance.com:9443/stream?streams={streams}"
        while True:
            try:
                async with websockets.connect(
                    url, ping_interval=self.PING_INTERVAL, ping_timeout=self.PING_TIMEOUT, max_size=self.MAX_SIZE
                ) as ws:
                    async for msg in ws:
                        try:
                            data = json.loads(msg)
                        except:
                            continue
                        stream = data.get("stream") or ""
                        d = data.get("data", {})
                        sym_lc = stream.split("@", 1)[0] if stream else ""
                        sym_uc = sym_lc.upper() if sym_lc else d.get("s")
                        # map back to pair
                        pair = next((p for p in batch if self._pair_to_binance(p) == sym_uc), None)
                        if not pair: 
                            continue
                        bids = d.get("bids") or d.get("b") or []
                        asks = d.get("asks") or d.get("a") or []
                        if not bids or not asks:
                            continue
                        try:
                            bpx_str, bs_str = bids[0][0], bids[0][1]
                            apx_str, as_str = asks[0][0], asks[0][1]
                            bpx, apx = float(bpx_str), float(apx_str)
                            bs,  a_s = float(bs_str), float(as_str)
                        except:
                            continue
                        q = _QuoteCompat(
                            bid=bpx, ask=apx,
                            bid_sz=bs, ask_sz=a_s,
                            bid_str=bpx_str, ask_str=apx_str,
                            ts_ms=now_ms()
                        )
                        if self.on_quote:
                            self.on_quote(pair, q)
            except Exception as e:
                print("[binance] reconnecting after error:", e)
                await asyncio.sleep(3)

    async def run(self, pairs: List[str]) -> None:
        if not pairs:
            return
        batches = chunked(sorted(pairs), self.SUB_BATCH)
        await asyncio.gather(*(self._consume(b) for b in batches))

# Entry point class for main.py
def MARKET_CLASS():
    return BinanceMarket()
