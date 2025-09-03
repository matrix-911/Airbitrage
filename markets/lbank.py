# markets/lbank.py
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

def _sym_lb(pair: str) -> str:
    # "ETH/USDT" -> "eth_usdt"
    return pair.replace("/", "_").lower()

class LBankMarket:
    name = "lbank"
    on_quote: Optional[Callable[[str, _QuoteCompat], None]] = None

    # Tunables
    SUB_BATCH = 35
    MAX_SIZE = 2**22
    PING_INTERVAL = 20
    PING_TIMEOUT = 20
    LBANK_DEPTH = "1"   # "10" | "50" | "100"
    WS_URL = "wss://www.lbkex.net/ws/V2/"

    # ---------- Discovery ----------
    async def discover(self, desired_pairs: List[str]) -> Set[str]:
        """
        Robust discovery across several endpoints. Returns subset of desired_pairs.
        """
        endpoints = [
            "https://api.lbkex.com/v2/currencyPairs.do",
            "https://api.lbkex.net/v2/currencyPairs.do",
            "https://www.lbkex.net/v2/currencyPairs.do",
            "https://api.lbkex.com/v2/currency_pairs.do",
            "https://api.lbkex.net/v2/currency_pairs.do",
        ]
        avail_syms: Set[str] = set()
        try:
            async with aiohttp.ClientSession() as session:
                for url in endpoints:
                    payload = await self._fetch_json(session, url)
                    syms = self._extract_pairs(payload)
                    if syms:
                        avail_syms = syms
                        # print(f"[lbank][discover] {url} -> {len(syms)}")
                        break
        except Exception as e:
            print("[lbank][discover] error:", e)

        ok: Set[str] = set()
        for p in desired_pairs:
            if _sym_lb(p) in avail_syms:
                ok.add(p)
        return ok

    @staticmethod
    async def _fetch_json(session: aiohttp.ClientSession, url: str):
        try:
            async with session.get(url, timeout=20) as r:
                txt = await r.text()  # sometimes text/plain
                try:
                    return json.loads(txt)
                except Exception:
                    try:
                        return await r.json(content_type=None)
                    except Exception:
                        return txt
        except Exception as e:
            print(f"[lbank][discover] fetch error {url}:", e)
            return None

    @staticmethod
    def _extract_pairs(payload) -> Set[str]:
        """
        Accepts:
          1) ["btc_usdt","eth_btc", ...]
          2) {"data":[{"symbol":"btc_usdt"}, ...]}
          3) "btc_usdt,eth_btc,..."
        Returns a set of lower-case url_symbols.
        """
        out: Set[str] = set()
        if payload is None:
            return out

        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, str):
                    out.add(item.strip().lower())
                elif isinstance(item, dict):
                    sym = (item.get("symbol") or item.get("pair") or "").strip().lower()
                    if sym:
                        out.add(sym)
            return out

        if isinstance(payload, dict):
            data = payload.get("data") or payload.get("pairs") or payload.get("result")
            if isinstance(data, list):
                for itm in data:
                    if isinstance(itm, str):
                        out.add(itm.strip().lower())
                    elif isinstance(itm, dict):
                        sym = (itm.get("symbol") or itm.get("pair") or "").strip().lower()
                        if sym:
                            out.add(sym)
            if not out:
                for key in ("pairs","symbols","currencyPairs","currency_pairs"):
                    val = payload.get(key)
                    if isinstance(val, str):
                        out |= {s.strip().lower() for s in val.split(",") if s.strip()}
            return out

        if isinstance(payload, str):
            return {s.strip().lower() for s in payload.split(",") if s.strip()}

        return out

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
        subs = [
            {"action":"subscribe","subscribe":"depth","depth":self.LBANK_DEPTH,"pair": _sym_lb(p)}
            for p in batch
        ]
        # per-batch order books
        books: Dict[str, Dict[str, Dict[str, float]]] = {p: {"bids": {}, "asks": {}} for p in batch}

        while True:
            try:
                async with websockets.connect(
                    self.WS_URL,
                    ping_interval=self.PING_INTERVAL,
                    ping_timeout=self.PING_TIMEOUT,
                    max_size=self.MAX_SIZE,
                ) as ws:
                    for sub in subs:
                        await ws.send(json.dumps(sub))

                    async for raw in ws:
                        try:
                            data = json.loads(raw)
                        except Exception:
                            continue

                        if data.get("action") == "ping":
                            try:
                                await ws.send(json.dumps({"action":"pong","pong": data.get("ping")}))
                            except:
                                pass
                            continue

                        if data.get("type") != "depth":
                            continue

                        pair_sym = data.get("pair") or ""
                        pair = next((p for p in batch if _sym_lb(p) == pair_sym), None)
                        if not pair:
                            continue
                        book = books[pair]

                        depth = data.get("depth") or {}
                        bids = depth.get("bids") or []
                        asks = depth.get("asks") or []

                        # full snapshot each push → rebuild
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
                print("[lbank] reconnecting after error:", e)
                await asyncio.sleep(3)

    # ---------- Runner ----------
    async def run(self, pairs: List[str]) -> None:
        if not pairs:
            return
        batches = chunked(sorted(pairs), self.SUB_BATCH)
        await asyncio.gather(*(self._consume(b) for b in batches))

# Entry point factory for main.py
def MARKET_CLASS():
    return LBankMarket()
