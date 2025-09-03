# markets/coinbase.py
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

def _sym_cb(pair: str) -> str:
    # "ETH/USDC" -> "ETH-USDC"
    return pair.replace("/", "-").upper()

class CoinbaseMarket:
    name = "coinbase"
    on_quote: Optional[Callable[[str, _QuoteCompat], None]] = None

    # Tunables
    SUB_BATCH = 60
    MAX_SIZE = 2**22
    PING_INTERVAL = 20
    PING_TIMEOUT = 20
    USE_L2_BATCH = True  # set True to reduce message rate (~5s cadence)
    REST_PRODUCTS = "https://api.exchange.coinbase.com/products"
    WS_URL = "wss://ws-feed.exchange.coinbase.com"

    # ---------- Discovery ----------
    async def discover(self, desired_pairs: List[str]) -> Set[str]:
        """
        Return subset of desired_pairs supported on Coinbase (advanced trade), filtering to online & tradable.
        """
        ok: Set[str] = set()
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.REST_PRODUCTS, timeout=20) as r:
                    j = await r.json()
            for prod in j if isinstance(j, list) else []:
                base = (prod.get("base_currency") or "").upper()
                quote = (prod.get("quote_currency") or "").upper()
                pair = f"{base}/{quote}"
                status = (prod.get("status") or "").lower()
                if pair in desired_pairs and status == "online" \
                   and not bool(prod.get("trading_disabled", False)) \
                   and not bool(prod.get("cancel_only", False)) \
                   and not bool(prod.get("post_only", False)):
                    ok.add(pair)
        except Exception as e:
            print("[coinbase][discover] error:", e)
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
        # per-batch order books: books[pair] = {"bids": {price_str: size}, "asks": {...}}
        books: Dict[str, Dict[str, Dict[str, float]]] = {p: {"bids": {}, "asks": {}} for p in batch}
        product_ids = [_sym_cb(p) for p in batch]
        channel_name = "level2_batch" if self.USE_L2_BATCH else "level2"
        sub_msg = {"type": "subscribe", "channels": [{"name": channel_name, "product_ids": product_ids}]}

        while True:
            try:
                async with websockets.connect(
                    self.WS_URL,
                    ping_interval=self.PING_INTERVAL,
                    ping_timeout=self.PING_TIMEOUT,
                    max_size=self.MAX_SIZE,
                ) as ws:
                    await ws.send(json.dumps(sub_msg))
                    # print(f"[coinbase][ws] subscribed {len(product_ids)} to {channel_name}")

                    async for raw in ws:
                        try:
                            data = json.loads(raw)
                        except Exception:
                            continue

                        t = data.get("type")
                        if t in ("subscriptions", "error"):
                            if t == "error":
                                print("[coinbase][ws][error]:", data)
                            continue
                        if t not in ("snapshot", "l2update"):
                            continue

                        pid = data.get("product_id") or ""
                        pair = next((p for p in batch if _sym_cb(p) == pid), None)
                        if not pair:
                            continue
                        book = books[pair]

                        if t == "snapshot":
                            # reset and load full levels
                            book['bids'].clear(); book['asks'].clear()
                            for pr, sz in data.get("bids", []):
                                try:
                                    price = float(pr); size = float(sz)
                                    self._set_level("buy", book, price, size)
                                except:
                                    pass
                            for pr, sz in data.get("asks", []):
                                try:
                                    price = float(pr); size = float(sz)
                                    self._set_level("sell", book, price, size)
                                except:
                                    pass

                        elif t == "l2update":
                            for side, pr, sz in data.get("changes", []):
                                side = side.lower()
                                try:
                                    price = float(pr); size = float(sz)
                                except:
                                    continue
                                self._set_level("buy" if side == "buy" else "sell", book, price, size)

                        bb, aa = self._best_levels(book)
                        if bb or aa:
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
                print("[coinbase] reconnecting after error:", e)
                await asyncio.sleep(3)

    # ---------- Runner ----------
    async def run(self, pairs: List[str]) -> None:
        if not pairs:
            return
        batches = chunked(sorted(pairs), self.SUB_BATCH)
        await asyncio.gather(*(self._consume(b) for b in batches))

# Entry point factory for main.py
def MARKET_CLASS():
    return CoinbaseMarket()
