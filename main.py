import asyncio, json, os, time, importlib, sys, math, contextlib
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Set, Callable
from decimal import Decimal, ROUND_DOWN, InvalidOperation, getcontext
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich import box
from rich.console import Console

# ===== config =====
import config
from get_all_coins import write_full_universe

console = Console()

# =========================
# ====== DATA TYPES =======
# =========================
@dataclass
class Quote:
    bid: Optional[float] = None
    ask: Optional[float] = None
    bid_sz: Optional[float] = None
    ask_sz: Optional[float] = None
    bid_str: Optional[str] = None
    ask_str: Optional[str] = None
    ts_ms: int = 0

def now_ms() -> int:
    return int(time.time() * 1000)

def age_sec(ts_ms: int) -> float:
    if ts_ms <= 0: return math.inf
    return max(0.0, (now_ms() - ts_ms) / 1000.0)

# =========================
# ====== FORMATTERS  ======
# =========================
MAX_DECIMALS = config.MAX_DECIMALS

def _pretty_number(value_str: Optional[str], value_float: Optional[float], max_dec: int = MAX_DECIMALS) -> str:
    
    getcontext().prec = max_dec + 8
    d: Optional[Decimal] = None

    # Try string first
    if value_str is not None:
        try:
            d = Decimal(value_str)
        except InvalidOperation:
            d = None

    # Fall back to float
    if d is None and (value_float is not None):
        # Guard non-finite floats like nan/inf
        try:
            import math as _math
            if isinstance(value_float, float) and not _math.isfinite(value_float):
                return "None"
            d = Decimal(str(value_float))
        except InvalidOperation:
            return "None"

    # If still bad or non-finite Decimal (NaN/Inf), bail out
    if d is None or not d.is_finite():
        return "None"

    # Quantize safely
    try:
        q = d.quantize(Decimal('1e-' + str(max_dec)), rounding=ROUND_DOWN)
    except InvalidOperation:
        return "None"

    s = format(q, 'f')  # never scientific notation
    if '.' in s:
        s = s.rstrip('0').rstrip('.')
    if s == "-0":
        s = "0"
    return s

def fmt_full(x_str: Optional[str], x_float: Optional[float]) -> str:
    return _pretty_number(x_str, x_float, MAX_DECIMALS)

# =========================
# ====== UTILITIES  =======
# =========================
def _load_universe(path: str) -> List[dict]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} not found")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    # Keep only entries that are active, have integer rank and a symbol
    data = [
        {
            "id": d.get("id"),
            "name": d.get("name"),
            "symbol": (d.get("symbol") or "").strip().upper(),
            "rank": d.get("rank"),
            "is_active": bool(d.get("is_active"))
        }
        for d in data
        if d.get("symbol") and isinstance(d.get("rank"), int) and d.get("is_active")
    ]
    data.sort(key=lambda d: d["rank"])
    return data

def load_symbols_universe(path: str, rank_range: Tuple[int,int], extra_symbols: List[str]) -> List[str]:
    data = _load_universe(path)
    lo, hi = rank_range
    if lo > hi:
        lo, hi = hi, lo

    # primary selection by rank
    chosen = [d for d in data if lo <= d["rank"] <= hi]
    symbols = {d["symbol"] for d in chosen}

    # force-include extras if present in the universe file
    extra_set = {s.strip().upper() for s in (extra_symbols or []) if s}
    for d in data:
        sym = d["symbol"]
        if sym in extra_set:
            symbols.add(sym)

    return sorted(symbols)

def make_pairs(bases: List[str], quotes: List[str]) -> List[str]:
    out, seen = [], set()
    for b in bases:
        for q in quotes:
            if b == q:
                continue
            p = f"{b}/{q}"
            if p not in seen:
                seen.add(p); out.append(p)
    return out

def pairs_key(pairs: List[str]) -> Tuple[str, ...]:
    """Stable key for comparing desired pair sets."""
    return tuple(sorted(set(pairs)))

# =========================
# ====== MARKET API =======
# =========================
class MarketBase:
    name: str = "base"
    on_quote: Optional[Callable[[str, Quote], None]] = None

    async def discover(self, desired_pairs: List[str]) -> Set[str]:
        raise NotImplementedError

    async def run(self, pairs: List[str]) -> None:
        raise NotImplementedError

# =========================
# ====== ARB STATE  =======
# =========================
@dataclass
class ArbState:
    in_window: bool = False
    since_ms: Optional[int] = None

arb_states: Dict[Tuple[str,str,str], ArbState] = {}

THRESH_ENTER = config.THRESH_ENTER_PCT / 100.0
THRESH_EXIT  = config.THRESH_EXIT_PCT  / 100.0

def update_hysteresis(key: Tuple[str,str,str], profit_frac: float, nowms: int) -> bool:
    st = arb_states.get(key)
    if st is None:
        st = ArbState(); arb_states[key] = st
    if not st.in_window:
        if profit_frac >= THRESH_ENTER:
            st.in_window = True
            st.since_ms = nowms
    else:
        if profit_frac < THRESH_EXIT:
            st.in_window = False
            st.since_ms = None
    return st.in_window

def is_long(key: Tuple[str,str,str], nowms: int) -> bool:
    st = arb_states.get(key)
    if not st or not st.in_window or st.since_ms is None:
        return False
    return (nowms - st.since_ms) >= (config.LONG_SECS * 1000)

# =========================
# ====== KEY INPUT   ======
# =========================
if sys.platform == "win32":
    import msvcrt
else:
    import termios, tty, select

class KeyReader:
    def __enter__(self):
        if sys.platform != "win32":
            self.fd = sys.stdin.fileno()
            self.old_settings = termios.tcgetattr(self.fd)
            tty.setcbreak(self.fd)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if sys.platform != "win32":
            termios.tcsetattr(self.fd, termios.TCSADRAIN, self.old_settings)

    async def read_key(self) -> str:
        await asyncio.sleep(0)
        if sys.platform == "win32":
            if msvcrt.kbhit():
                ch = msvcrt.getch()
                if ch in (b'\xe0', b'\x00'):
                    ch2 = msvcrt.getch()
                    if ch2 == b'K': return "LEFT"
                    if ch2 == b'M': return "RIGHT"
                    if ch2 == b'H': return "UP"
                    if ch2 == b'P': return "DOWN"
                    return ""
                try:
                    k = ch.decode(errors="ignore").lower()
                except:
                    return ""
                if k == "a": return "A"
                if k == "s": return "S"
                if k == "l": return "L"
                if k == "q": return "Q"
        else:
            dr, _, _ = select.select([sys.stdin], [], [], 0)
            if not dr:
                return ""
            ch1 = os.read(self.fd, 1).decode(errors="ignore")
            if ch1 == "\x1b":
                if select.select([sys.stdin], [], [], 0)[0]:
                    ch2 = os.read(self.fd, 1).decode(errors="ignore")
                    if ch2 == "[" and select.select([sys.stdin], [], [], 0)[0]:
                        ch3 = os.read(self.fd, 1).decode(errors="ignore")
                        if ch3 == "D": return "LEFT"
                        if ch3 == "C": return "RIGHT"
                        if ch3 == "A": return "UP"
                        if ch3 == "B": return "DOWN"
                return ""
            k = ch1.lower()
            if k == "a": return "A"
            if k == "s": return "S"
            if k == "l": return "L"
            if k == "q": return "Q"
        return ""

# =========================
# ====== MAIN APP    ======
# =========================
class App:
    def __init__(self):
        self.markets: Dict[str, MarketBase] = {}
        self.prices: Dict[str, Dict[str, Quote]] = {}
        self.supported: Dict[str, Set[str]] = {}
        self.view: str = "active"
        self.page: int = 0

        # dynamic state for hot-reload
        self.current_pairs_key: Tuple[str, ...] = tuple()
        self.market_task: Optional[asyncio.Task] = None
        self.refresher_task: Optional[asyncio.Task] = None
        self.stop_event = asyncio.Event()

    def on_quote(self, market: str, pair: str, q: Quote):
        self.prices.setdefault(market, {})[pair] = q

    async def load_markets(self):
        for name in config.MARKETS_TO_USE:
            mod = importlib.import_module(f"markets.{name}")
            market: MarketBase = mod.MARKET_CLASS()
            market.on_quote = lambda pair, q, mkt=name: self.on_quote(mkt, pair, q)
            self.markets[name] = market
            self.prices[name] = {}

    async def discover(self, desired_pairs: List[str]):
        self.supported.clear()
        for name, mkt in self.markets.items():
            ok = await mkt.discover(desired_pairs)
            self.supported[name] = ok
            # initialize quotes
            self.prices[name] = {p: self.prices.get(name, {}).get(p, Quote()) for p in ok}

    async def _run_markets_once(self):
        tasks = []
        for name, mkt in self.markets.items():
            pairs = sorted(list(self.supported.get(name, set())))
            if not pairs:
                console.print(f"[bold red]No supported pairs for {name}[/]")
                continue
            tasks.append(asyncio.create_task(mkt.run(pairs)))
        if not tasks:
            console.print("[bold red]Nothing to run.[/]")
            return
        await asyncio.gather(*tasks)

    async def start_markets(self):
        # cancel previous markets task if running
        if self.market_task and not self.market_task.done():
            self.market_task.cancel()
            with contextlib.suppress(Exception):
                await self.market_task
        # start new
        self.market_task = asyncio.create_task(self._run_markets_once())

    # ---------- Arbitrage computation ----------
    def compute_arbitrages(self) -> List[dict]:
        markets = list(self.markets.keys())
        pairs_all: Set[str] = set()
        for m in markets:
            pairs_all.update(self.prices.get(m, {}).keys())

        ops = []
        nms = now_ms()

        for pair in pairs_all:
            avail = []
            for m in markets:
                q = self.prices.get(m, {}).get(pair)
                if not q or q.ask is None or q.bid is None:
                    continue
                avail.append((m, q))
            if len(avail) < 2:
                continue

            for m_buy, q_buy in avail:
                if q_buy.ask is None or q_buy.ask_sz is None:
                    continue
                for m_sell, q_sell in avail:
                    if m_sell == m_buy or q_sell.bid is None or q_sell.bid_sz is None:
                        continue
                    profit_frac = (q_sell.bid - q_buy.ask) / q_buy.ask
                    profit_pct = profit_frac * 100.0
                    if profit_pct > config.MAX_PROFIT_PCT:
                        continue

                    key = (pair, m_buy, m_sell)
                    active = update_hysteresis(key, profit_frac, nms)
                    if not active:
                        continue

                    buy_qty  = q_buy.ask_sz or 0.0
                    sell_qty = q_sell.bid_sz or 0.0
                    exec_qty = min(buy_qty, sell_qty)

                    op = {
                        "pair": pair,
                        "buy_mkt": m_buy,
                        "sell_mkt": m_sell,
                        "buy_price": q_buy.ask,
                        "sell_price": q_sell.bid,
                        "buy_price_str": q_buy.ask_str,
                        "sell_price_str": q_sell.bid_str,
                        "profit_pct": profit_pct,
                        "buy_qty": buy_qty,
                        "sell_qty": sell_qty,
                        "exec_qty": exec_qty,
                        "qty": exec_qty,
                        "buy_age": age_sec(q_buy.ts_ms),
                        "sell_age": age_sec(q_sell.ts_ms),
                        "long": is_long(key, nms),
                    }
                    ops.append(op)

        ops.sort(key=lambda d: d["profit_pct"], reverse=True)
        return ops

    def list_stale(self) -> List[Tuple[str, str, float, Quote]]:
        stale = []
        for mkt, pairs in self.prices.items():
            for pair, q in pairs.items():
                a = age_sec(q.ts_ms)
                if a >= config.STALE_SECS:
                    stale.append((mkt, pair, a, q))
        stale.sort(key=lambda x: (-x[2], x[0], x[1]))
        return stale

    # ---------- UI ----------
    def render_active(self, ops: List[dict]) -> Table:
        title = (f"ACTIVE ARBITRAGE (enter ≥ {config.THRESH_ENTER_PCT:.2f}%, "
                 f"exit < {config.THRESH_EXIT_PCT:.2f}%) — arrows: page | A=Active S=Stale L=Long Q=Quit")
        table = Table(title=title, box=box.MINIMAL_HEAVY_HEAD, show_lines=False)
        table.add_column("#", justify="right", width=3)
        table.add_column("PAIR", justify="left")
        table.add_column("BUY@MKT", justify="left")
        table.add_column("BUY PRICE", justify="right")
        table.add_column("SELL@MKT", justify="left")
        table.add_column("SELL PRICE", justify="right")
        table.add_column("PROFIT %", justify="right")
        table.add_column("BUY AMT", justify="right")
        table.add_column("SELL AMT", justify="right")
        table.add_column("EXEC AMT", justify="right")
        table.add_column("AGES (b/s)", justify="right")
        table.add_column("LONG", justify="center", width=6)

        start = self.page * config.PAGE_SIZE
        chunk = ops[start:start + config.PAGE_SIZE]
        for i, op in enumerate(chunk, start=1+start):
            profit_col = f"[bold green]{op['profit_pct']:.4f}[/]"
            buy_px = fmt_full(op["buy_price_str"], op["buy_price"])
            sell_px = fmt_full(op["sell_price_str"], op["sell_price"])
            age_col = f"{op['buy_age']:.1f}s/{op['sell_age']:.1f}s"
            long_col = "[yellow]YES[/]" if op["long"] else ""
            table.add_row(
                str(i),
                op["pair"],
                op["buy_mkt"],
                buy_px,
                op["sell_mkt"],
                sell_px,
                profit_col,
                fmt_full(None, op["buy_qty"]),
                fmt_full(None, op["sell_qty"]),
                fmt_full(None, op["exec_qty"]),
                age_col,
                long_col
            )
        if not chunk:
            table.add_row("-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-")
        total_pages = max(1, math.ceil(len(ops) / config.PAGE_SIZE))
        table.caption = f"Page {self.page+1}/{total_pages} • Rows: {len(ops)}"
        return table

    def render_stale(self, items: List[Tuple[str,str,float,Quote]]) -> Table:
        title = (f"STALE (no update ≥ {config.STALE_SECS//60} min) — arrows: page | "
                 f"A=Active S=Stale L=Long Q=Quit")
        table = Table(title=title, box=box.MINIMAL_HEAVY_HEAD)
        table.add_column("#", justify="right", width=3)
        table.add_column("MARKET", justify="left")
        table.add_column("PAIR", justify="left")
        table.add_column("BID", justify="right")
        table.add_column("ASK", justify="right")
        table.add_column("AGE", justify="right")

        start = self.page * config.PAGE_SIZE
        chunk = items[start:start + config.PAGE_SIZE]
        for i, (mkt, pair, a, q) in enumerate(chunk, start=1+start):
            table.add_row(
                str(i), mkt, pair,
                fmt_full(q.bid_str, q.bid),
                fmt_full(q.ask_str, q.ask),
                f"{a:.1f}s"
            )
        if not chunk:
            table.add_row("-", "-", "-", "-", "-", "-")
        total_pages = max(1, math.ceil(len(items) / config.PAGE_SIZE))
        table.caption = f"Page {self.page+1}/{total_pages} • Rows: {len(items)}"
        return table

    def render_long(self, ops: List[dict]) -> Table:
        long_ops = [op for op in ops if op["long"]]
        title = (f"LONG ARBITRAGE (≥{config.THRESH_ENTER_PCT:.2f}% & not ≤{config.THRESH_EXIT_PCT:.2f}% "
                 f"for ≥ {config.LONG_SECS//60} min) — arrows: page | A=Active S=Stale L=Long Q=Quit")
        table = Table(title=title, box=box.MINIMAL_HEAVY_HEAD, show_lines=False)
        table.add_column("#", justify="right", width=3)
        table.add_column("PAIR", justify="left")
        table.add_column("BUY@MKT", justify="left")
        table.add_column("BUY PRICE", justify="right")
        table.add_column("SELL@MKT", justify="left")
        table.add_column("SELL PRICE", justify="right")
        table.add_column("PROFIT %", justify="right")
        table.add_column("BUY AMT", justify="right")
        table.add_column("SELL AMT", justify="right")
        table.add_column("EXEC AMT", justify="right")
        table.add_column("AGES (b/s)", justify="right")

        start = self.page * config.PAGE_SIZE
        chunk = long_ops[start:start + config.PAGE_SIZE]
        for i, op in enumerate(chunk, start=1+start):
            buy_px = fmt_full(op["buy_price_str"], op["buy_price"])
            sell_px = fmt_full(op["sell_price_str"], op["sell_price"])
            table.add_row(
                str(i),
                op["pair"],
                op["buy_mkt"],
                buy_px,
                op["sell_mkt"],
                sell_px,
                f"[bold green]{op['profit_pct']:.4f}[/]",
                fmt_full(None, op["buy_qty"]),
                fmt_full(None, op["sell_qty"]),
                fmt_full(None, op["exec_qty"]),
                f"{op['buy_age']:.1f}s/{op['sell_age']:.1f}s"
            )
        if not chunk:
            table.add_row("-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-")
        total_pages = max(1, math.ceil(len(long_ops) / config.PAGE_SIZE))
        table.caption = f"Page {self.page+1}/{total_pages} • Rows: {len(long_ops)}"
        return table

    # ---------- Universe refresh / hot-reload ----------
    async def periodic_universe_refresh(self):
        """Background task: refresh raw universe file periodically and hot-reload pairs if needed."""
        # If disabled, just return immediately (task won't be started anyway)
        if not getattr(config, "REFRESH_UNIVERSE_ENABLED", True):
            return

        hours = max(1, int(config.UNIVERSE_REFRESH_HOURS))  # at least 1 hour
        interval = hours * 3600
        while not self.stop_event.is_set():
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=interval)
                if self.stop_event.is_set():
                    break
            except asyncio.TimeoutError:
                pass  # time to refresh

            # 1) fetch & write full universe
            try:
                console.print("[yellow]Auto-refreshing coins_universe.json...[/]")
                count = write_full_universe(
                    path=config.COINS_UNIVERSE_FILE,
                    timeout=config.COINPAPRIKA_TIMEOUT,
                )
                console.print(f"[green]Universe refreshed ({count} coins).[/]")
            except Exception as e:
                console.print(f"[bold red]Universe refresh failed:[/] {e}")
                continue  # keep running with old universe

            # 2) recompute desired pairs
            new_bases = load_symbols_universe(
                config.COINS_UNIVERSE_FILE,
                config.COINS_RANK_RANGE,
                config.EXTRA_BASE_SYMBOLS
            )
            new_pairs = make_pairs(new_bases, config.QUOTE_CURRENCIES)
            new_key = pairs_key(new_pairs)

            # 3) if universe changed, rediscover & restart markets
            if new_key != self.current_pairs_key:
                console.print("[magenta]Universe changed — reconfiguring markets...[/]")
                await self.discover(new_pairs)
                await self.start_markets()
                self.current_pairs_key = new_key
                console.print("[magenta]Markets reconfigured.[/]")
            else:
                console.print("[cyan]Universe changed but desired pairs unchanged — no restart needed.[/]")

    async def ui_loop(self):
        refresh_hz = max(1, int(1000 / config.UI_REFRESH_MS))
        with Live(refresh_per_second=refresh_hz, screen=True) as live, KeyReader() as kr:
            while True:
                ops = self.compute_arbitrages()
                if self.view == "active":
                    live.update(Panel(self.render_active(ops)))
                elif self.view == "stale":
                    live.update(Panel(self.render_stale(self.list_stale())))
                else:
                    live.update(Panel(self.render_long(ops)))

                key = await kr.read_key()
                if key == "LEFT":
                    self.page = max(0, self.page - 1)
                elif key == "RIGHT":
                    self.page += 1
                elif key == "A":
                    self.view, self.page = "active", 0
                elif key == "S":
                    self.view, self.page = "stale", 0
                elif key == "L":
                    self.view, self.page = "long", 0
                elif key == "Q":
                    self.stop_event.set()
                    break

                await asyncio.sleep(config.UI_REFRESH_MS / 1000.0)

    async def run(self):
        # 0) initial universe handling
        if getattr(config, "REFRESH_UNIVERSE_ENABLED", True):
            try:
                console.print("[yellow]Fetching initial coins_universe.json...[/]")
                write_full_universe(
                    path=config.COINS_UNIVERSE_FILE,
                    timeout=config.COINPAPRIKA_TIMEOUT,
                )
                console.print("[green]Initial universe fetched.[/]")
            except Exception as e:
                console.print(f"[bold red]Initial universe fetch failed:[/] {e}")
                if not os.path.exists(config.COINS_UNIVERSE_FILE):
                    console.print("[bold red]No local coins_universe.json available. Exiting.[/]")
                    return
        else:
            console.print("[cyan]Refresh disabled — using existing coins_universe.json only.[/]")
            if not os.path.exists(config.COINS_UNIVERSE_FILE):
                console.print("[bold red]coins_universe.json not found and refresh is disabled. Exiting.[/]")
                return

        # 1) load market modules
        await self.load_markets()

        # 2) build initial pairs
        bases = load_symbols_universe(
            config.COINS_UNIVERSE_FILE,
            config.COINS_RANK_RANGE,
            config.EXTRA_BASE_SYMBOLS
        )
        lo, hi = config.COINS_RANK_RANGE
        console.print(f"[cyan]Loaded bases by rank range [{lo}-{hi}] (+extras): {len(bases)}[/]")
        desired_pairs = make_pairs(bases, config.QUOTE_CURRENCIES)
        console.print(f"[cyan]Desired pairs: {len(desired_pairs)}[/]")
        self.current_pairs_key = pairs_key(desired_pairs)

        # 3) discover & start markets
        await self.discover(desired_pairs)
        await self.start_markets()

        # 4) start background refresher + UI (only if enabled)
        if getattr(config, "REFRESH_UNIVERSE_ENABLED", True):
            self.refresher_task = asyncio.create_task(self.periodic_universe_refresh())

        try:
            await self.ui_loop()  # blocks until 'Q'
        finally:
            self.stop_event.set()
            # cancel background & markets
            if self.refresher_task:
                self.refresher_task.cancel()
            if self.market_task and not self.market_task.done():
                self.market_task.cancel()

            # best-effort cleanup
            with contextlib.suppress(Exception):
                if self.refresher_task: await self.refresher_task
            with contextlib.suppress(Exception):
                if self.market_task: await self.market_task

# =========================
# ========= BOOT ==========
# =========================
if __name__ == "__main__":
    try:
        asyncio.run(App().run())
    except KeyboardInterrupt:
        print("\nShutting down...")
