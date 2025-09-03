# markets/bybit.py
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

def _sym_bb(pair: str) -> str:
    # "BTC/USDT" -> "BTCUSDT"
    return pair.replace("/", "").upper()

class BybitMarket:
    name = "bybit"
    on_quote: Optional[Callable[[str, _QuoteCompat], None]] = None

    # Tunables
    SUB_BATCH = 10                     # topics per socket
    MAX_SIZE = 2**22
    PING_INTERVAL = 20
    PING_TIMEOUT = 20
    DEPTH = 1                          # allowed: 1, 50, 200, 1000
    WS_URL = "wss://stream.bybit.com/v5/public/spot"
    REST_INSTR = "https://api.bybit.com/v5/market/instruments-info?category=spot"

    # ---------- Discovery ----------
    async def discover(self, desired_pairs: List[str]) -> Set[str]:
        """
        Return subset of desired_pairs supported on Bybit Spot.
        """
        ok: Set[str] = set()
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.REST_INSTR, timeout=20) as r:
                    j = await r.json()
            for it in (j.get("result", {}) or {}).get("list", []) or []:
                base = (it.get("baseCoin") or "").upper()
                quote = (it.get("quoteCoin") or "").upper()
                status = (it.get("status") or "").title()  # "Trading"
                pair = f"{base}/{quote}"
                if pair in desired_pairs and status == "Trading":
                    ok.add(pair)
        except Exception as e:
            print("[bybit][discover] error:", e)
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
        bids, asks = book['bids'], book['asks']
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
        symbols = [_sym_bb(p) for p in batch]
        args = [f"orderbook.{self.DEPTH}.{s}" for s in symbols]
        sub_msg = {"op": "subscribe", "args": args}

        # per-batch book store: {"PAIR": {"bids": {price_str: size}, "asks": {...}}}
        books: Dict[str, Dict[str, Dict[str, float]]] = {p: {"bids": {}, "asks": {}} for p in batch}

        while True:
            try:
                async with websockets.connect(
                    self.WS_URL,
                    ping_interval=self.PING_INTERVAL,
                    ping_timeout=self.PING_TIMEOUT,
                    max_size=self.MAX_SIZE,
                ) as ws:
                    await ws.send(json.dumps(sub_msg))
                    # print(f"[bybit][ws] subscribe {len(args)} topics @ depth {self.DEPTH}")

                    async for raw in ws:
                        try:
                            data = json.loads(raw)
                        except Exception:
                            continue

                        # acks / errors / ping
                        if data.get("op") == "subscribe" and data.get("success") is True:
                            continue
                        if data.get("op") == "error" or data.get("retCode") not in (None, 0):
                            if "retCode" in data:
                                print("[bybit][ws][error]:", data)
                            continue
                        if data.get("op") == "ping":
                            await ws.send(json.dumps({"op": "pong", "req_id": data.get("req_id")}))
                            continue

                        topic = data.get("topic") or ""
                        typ = data.get("type") or ""
                        payload = data.get("data") or {}

                        if not topic or not typ or not payload:
                            continue
                        try:
                            _, _, symbol = topic.split(".", 2)  # orderbook.{depth}.{symbol}
                        except Exception:
                            continue

                        pair = next((p for p in batch if _sym_bb(p) == symbol), None)
                        if not pair:
                            continue
                        book = books[pair]

                        bids = payload.get("b") or []
                        asks = payload.get("a") or []

                        if typ == "snapshot":
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
                        elif typ == "delta":
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
                        else:
                            continue

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
                print("[bybit] reconnecting after error:", e)
                await asyncio.sleep(3)

    # ---------- Runner ----------
    async def run(self, pairs: List[str]) -> None:
        if not pairs:
            return
        batches = chunked(sorted(pairs), self.SUB_BATCH)
        await asyncio.gather(*(self._consume(b) for b in batches))

# Entry point factory for main.py
def MARKET_CLASS():
    return BybitMarket()
