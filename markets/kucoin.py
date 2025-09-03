# markets/kucoin.py
import asyncio, aiohttp, websockets, json, time, uuid
from typing import List, Set, Dict, Optional, Callable

# -- Quote-compatible object (same fields main expects) --
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

def human_to_kucoin(pair: str) -> str:
    b, q = pair.split("/")
    return f"{b}-{q}"

def kucoin_to_human(symbol: str) -> str:
    try:
        b, q = symbol.split("-")
        return f"{b}/{q}"
    except:
        return symbol

class KucoinMarket:
    name = "kucoin"
    on_quote: Optional[Callable[[str, _QuoteCompat], None]] = None

    # Tunables
    SUB_BATCH = 50           # KuCoin is touchy; keep batches modest
    MAX_SIZE = 2**22

    # ========= Discovery =========
    async def discover(self, desired_pairs: List[str]) -> Set[str]:
        """
        Return subset of desired_pairs that are enabled for trading on KuCoin.
        Also builds self._pair_to_symbol mapping (e.g., BTC/USDT -> BTC-USDT)
        """
        url = "https://api.kucoin.com/api/v2/symbols"
        ok: Set[str] = set()
        mapping: Dict[str, str] = {}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=25) as r:
                    j = await r.json()
            for d in (j.get("data") or []):
                if not d.get("enableTrading", False):
                    continue
                base = (d.get("baseCurrency") or "").upper()
                quote = (d.get("quoteCurrency") or "").upper()
                human = f"{base}/{quote}"
                if human in desired_pairs:
                    ok.add(human)
                    mapping[human] = d.get("symbol")  # e.g. BTC-USDT
        except Exception as e:
            print("[kucoin][discover] error:", e)

        self._pair_to_symbol = mapping
        return ok

    # ========= WS Bootstrap =========
    async def _get_ws_endpoint(self, session: aiohttp.ClientSession):
        """
        Returns (endpoint, token, pingIntervalMs).
        """
        url = "https://api.kucoin.com/api/v1/bullet-public"
        async with session.post(url, timeout=25) as r:
            j = await r.json()
        d = j.get("data", {})
        token = d.get("token")
        servers = d.get("instanceServers") or []
        if not token or not servers:
            raise RuntimeError(f"Bad bullet response: {j}")
        s = servers[0]
        return s["endpoint"], token, int(s.get("pingInterval", 20000))

    # ========= Consumer =========
    async def _consume(self, batch: List[str]):
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    endpoint, token, app_ping_ms = await self._get_ws_endpoint(session)
                    connect_id = str(uuid.uuid4())
                    url = f"{endpoint}?token={token}&connectId={connect_id}"

                    async with websockets.connect(url, max_size=self.MAX_SIZE) as ws:
                        # Subscribe for each pair in the batch
                        for p in batch:
                            sym = self._pair_to_symbol.get(p)
                            if not sym:
                                continue
                            sub = {
                                "id": str(uuid.uuid4()),
                                "type": "subscribe",
                                "topic": f"/spotMarket/level2Depth5:{sym}",
                                "privateChannel": False,
                                "response": True
                            }
                            await ws.send(json.dumps(sub))

                        # App-level ping task (KuCoin requires client pings)
                        async def pinger():
                            # keep a small margin below server interval
                            sleep_s = max(5, app_ping_ms/1000 - 2)
                            while True:
                                await asyncio.sleep(sleep_s)
                                try:
                                    await ws.send(json.dumps({"id": str(uuid.uuid4()), "type": "ping"}))
                                except:
                                    return
                        ping_task = asyncio.create_task(pinger())

                        try:
                            async for raw in ws:
                                try:
                                    m = json.loads(raw)
                                except:
                                    continue

                                # control msgs
                                if isinstance(m, dict) and m.get("type") in ("welcome","pong","ack"):
                                    # you may print acks if debugging:
                                    # if m["type"] == "ack": print("[kucoin][ack]", m)
                                    continue
                                if isinstance(m, dict) and m.get("type") == "error":
                                    print("[kucoin][error]", m)
                                    continue

                                # market data
                                if isinstance(m, dict) and m.get("type") == "message":
                                    topic = m.get("topic") or ""
                                    if not topic.startswith("/spotMarket/level2Depth5:"):
                                        continue
                                    data = m.get("data") or {}
                                    sym = topic.split(":", 1)[1]
                                    pair = kucoin_to_human(sym)
                                    if pair not in batch:
                                        continue

                                    bids = data.get("bids") or []
                                    asks = data.get("asks") or []
                                    # keep raw strings to preserve full precision
                                    try:
                                        if bids and bids[0]:
                                            bpx_str, bsz_str = bids[0][0], bids[0][1]
                                            bpx = float(bpx_str); bsz = float(bsz_str)
                                        else:
                                            bpx = bsz = bpx_str = bsz_str = None
                                        if asks and asks[0]:
                                            apx_str, asz_str = asks[0][0], asks[0][1]
                                            apx = float(apx_str); asz = float(asz_str)
                                        else:
                                            apx = asz = apx_str = asz_str = None
                                    except:
                                        continue

                                    if bpx is None and apx is None:
                                        continue

                                    q = _QuoteCompat(
                                        bid=bpx, ask=apx,
                                        bid_sz=bsz, ask_sz=asz,
                                        bid_str=bpx_str, ask_str=apx_str,
                                        ts_ms=now_ms()
                                    )
                                    if self.on_quote:
                                        self.on_quote(pair, q)
                        finally:
                            ping_task.cancel()
                except Exception as e:
                    print("[kucoin] reconnecting after error:", e)
                    await asyncio.sleep(3)

    # ========= Runner =========
    async def run(self, pairs: List[str]) -> None:
        # build symbol cache for run() in case discover() wasn't called directly (main does call it)
        self._pair_to_symbol = getattr(self, "_pair_to_symbol", {})
        for p in pairs:
            if p not in self._pair_to_symbol:
                self._pair_to_symbol[p] = human_to_kucoin(p)

        if not pairs:
            return
        batches = chunked(sorted(pairs), self.SUB_BATCH)
        await asyncio.gather(*(self._consume(b) for b in batches))

# Entry point factory for main.py
def MARKET_CLASS():
    return KucoinMarket()
