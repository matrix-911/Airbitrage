# markets/okx.py
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

def _sym_okx(pair: str) -> str:
    # "BTC/USDT" -> "BTC-USDT"
    b, q = pair.split("/")
    return f"{b}-{q}".upper()

def _human_pair(inst_id: str) -> str:
    try:
        b, q = inst_id.split("-")
        return f"{b}/{q}"
    except:
        return inst_id

class OkxMarket:
    name = "okx"
    on_quote: Optional[Callable[[str, _QuoteCompat], None]] = None

    # Tunables
    SUB_BATCH = 75
    MAX_SIZE = 2**22
    PING_INTERVAL = 20
    PING_TIMEOUT = 20
    OKX_BOOK_CHANNEL = "books5"  # "books", "books5", or "books-l2-tbt"
    REST_INSTRUMENTS = "https://www.okx.com/api/v5/public/instruments?instType=SPOT"
    WS_URL = "wss://ws.okx.com:8443/ws/v5/public"

    # ---------- Discovery ----------
    async def discover(self, desired_pairs: List[str]) -> Set[str]:
        """
        Return subset of desired_pairs supported on OKX Spot, and cache pair->instId.
        """
        ok: Set[str] = set()
        mapping: Dict[str, str] = {}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.REST_INSTRUMENTS, timeout=25) as r:
                    j = await r.json()
            data = j.get("data", []) or []
            desired = set(desired_pairs)
            for inst in data:
                inst_id = inst.get("instId") or ""
                if not inst_id:
                    continue
                human = _human_pair(inst_id)
                if human in desired:
                    ok.add(human)
                    mapping[human] = inst_id
        except Exception as e:
            print("[okx][discover] error:", e)

        self._pair_to_okx = mapping
        return ok

    # ---------- Consumer ----------
    async def _consume(self, batch: List[str]):
        while True:
            try:
                async with websockets.connect(
                    self.WS_URL,
                    ping_interval=self.PING_INTERVAL,
                    ping_timeout=self.PING_TIMEOUT,
                    max_size=self.MAX_SIZE,
                ) as ws:
                    args = []
                    for p in batch:
                        inst = self._pair_to_okx.get(p)
                        if inst:
                            args.append({"channel": self.OKX_BOOK_CHANNEL, "instId": inst})
                    if not args:
                        return

                    sub = {"op": "subscribe", "args": args}
                    await ws.send(json.dumps(sub))

                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue

                        # Ignore control events/acks
                        if isinstance(msg, dict) and "event" in msg:
                            continue

                        if not (isinstance(msg, dict) and "arg" in msg and "data" in msg):
                            continue

                        inst_id = msg["arg"].get("instId")
                        if not inst_id:
                            continue
                        pair = _human_pair(inst_id)
                        if pair not in batch:
                            continue

                        data_list = msg.get("data") or []
                        if not data_list:
                            continue
                        d0 = data_list[-1]  # take latest snapshot in the array

                        bids = d0.get("bids") or []
                        asks = d0.get("asks") or []

                        # OKX books entries are arrays like [price, size, ..., ...] (strings)
                        bid_px = bid_sz = ask_px = ask_sz = None
                        bid_px_str = bid_sz_str = ask_px_str = ask_sz_str = None
                        try:
                            if bids and bids[0]:
                                bid_px_str = str(bids[0][0])
                                bid_sz_str = str(bids[0][1])
                                bid_px = float(bid_px_str)
                                bid_sz = float(bid_sz_str)
                            if asks and asks[0]:
                                ask_px_str = str(asks[0][0])
                                ask_sz_str = str(asks[0][1])
                                ask_px = float(ask_px_str)
                                ask_sz = float(ask_sz_str)
                        except Exception:
                            # if parsing fails, skip this frame
                            continue

                        if bid_px is None and ask_px is None:
                            continue

                        q = _QuoteCompat(
                            bid=bid_px, ask=ask_px,
                            bid_sz=bid_sz, ask_sz=ask_sz,
                            bid_str=bid_px_str, ask_str=ask_px_str,
                            ts_ms=now_ms(),
                        )
                        if self.on_quote:
                            self.on_quote(pair, q)

            except Exception as e:
                print("[okx] reconnecting after error:", e)
                await asyncio.sleep(3)

    # ---------- Runner ----------
    async def run(self, pairs: List[str]) -> None:
        # ensure mapping present even if discover() skipped
        self._pair_to_okx = getattr(self, "_pair_to_okx", {})
        for p in pairs:
            if p not in self._pair_to_okx:
                self._pair_to_okx[p] = _sym_okx(p)

        if not pairs:
            return
        batches = chunked(sorted(pairs), self.SUB_BATCH)
        await asyncio.gather(*(self._consume(b) for b in batches))

# Entry point factory for main.py
def MARKET_CLASS():
    return OkxMarket()
