# markets/htx.py
import asyncio, aiohttp, websockets, json, time, gzip
from typing import List, Set, Dict, Optional, Callable, Tuple

# Quote-compatible object (same attributes main expects)
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

def _sym_htx(pair: str) -> str:
    # "BTC/USDT" -> "btcusdt"
    return pair.replace("/", "").lower()

class HtxMarket:
    name = "htx"
    on_quote: Optional[Callable[[str, _QuoteCompat], None]] = None

    # Tunables
    SUB_BATCH = 61
    MAX_SIZE = 2**23
    PING_INTERVAL = 20
    PING_TIMEOUT = 20

    # ========= Discovery =========
    async def discover(self, desired_pairs: List[str]) -> Set[str]:
        """
        Return subset of desired_pairs that are online tradable on HTX (Huobi spot).
        """
        url = "https://api.huobi.pro/v1/common/symbols"
        ok: Set[str] = set()
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=20) as r:
                    j = await r.json()
            for it in j.get("data", []) or []:
                if (it.get("state") or "").lower() != "online":
                    continue
                base = (it.get("base-currency") or "").upper()
                quote = (it.get("quote-currency") or "").upper()
                human = f"{base}/{quote}"
                if human in desired_pairs:
                    ok.add(human)
        except Exception as e:
            print("[htx][discover] error:", e)
        return ok

    # ========= Consumer =========
    async def _consume(self, batch: List[str]):
        url = "wss://api.huobi.pro/ws"
        # build subscriptions
        subs = [{"sub": f"market.{_sym_htx(p)}.depth.step0", "id": f"sub-{_sym_htx(p)}"} for p in batch]

        while True:
            try:
                async with websockets.connect(
                    url, ping_interval=self.PING_INTERVAL, ping_timeout=self.PING_TIMEOUT, max_size=self.MAX_SIZE
                ) as ws:
                    # subscribe all channels in this batch
                    for msg in subs:
                        await ws.send(json.dumps(msg))

                    async for raw in ws:
                        # frames are often gzip-compressed bytes
                        if isinstance(raw, (bytes, bytearray)):
                            try:
                                txt = gzip.decompress(raw).decode()
                            except Exception:
                                try:
                                    txt = raw.decode()
                                except Exception:
                                    continue
                        else:
                            txt = raw

                        try:
                            data = json.loads(txt)
                        except Exception:
                            continue

                        # ping/pong
                        if "ping" in data:
                            try:
                                await ws.send(json.dumps({"pong": data["ping"]}))
                            except:
                                pass
                            continue

                        # sub acks / errors
                        if data.get("status") == "error":
                            print("[htx][error]", data)
                            continue
                        if data.get("status") == "ok" and data.get("subbed"):
                            # subscribed ack
                            continue

                        ch = data.get("ch") or ""
                        tick = data.get("tick") or {}
                        if not ch or not tick:
                            continue

                        # channel looks like: market.btcusdt.depth.step0
                        try:
                            sym = ch.split(".")[1]
                        except Exception:
                            continue

                        # map back to pair in this batch
                        pair = next((p for p in batch if _sym_htx(p) == sym), None)
                        if not pair:
                            continue

                        bids = tick.get("bids") or []
                        asks = tick.get("asks") or []

                        # take top-of-book; HTX step0 snapshot is sorted (bids desc, asks asc)
                        bid_px = bid_sz = ask_px = ask_sz = None
                        bid_px_str = bid_sz_str = ask_px_str = ask_sz_str = None

                        if bids and bids[0]:
                            bid_px_str = str(bids[0][0]); bid_sz_str = str(bids[0][1])
                            try:
                                bid_px = float(bid_px_str); bid_sz = float(bid_sz_str)
                            except:
                                bid_px = bid_sz = None
                        if asks and asks[0]:
                            ask_px_str = str(asks[0][0]); ask_sz_str = str(asks[0][1])
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
                print("[htx] reconnecting after error:", e)
                await asyncio.sleep(3)

    # ========= Runner =========
    async def run(self, pairs: List[str]) -> None:
        if not pairs:
            return
        batches = chunked(sorted(pairs), self.SUB_BATCH)
        await asyncio.gather(*(self._consume(b) for b in batches))

# Entry point for main.py
def MARKET_CLASS():
    return HtxMarket()
