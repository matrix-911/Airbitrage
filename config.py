# =========================
# ====== CONFIG FILE ======
# =========================

# Where the full raw coin list is stored (from CoinPaprika /v1/coins)
COINS_UNIVERSE_FILE = "coins_universe.json"

# Use only this rank range at runtime (inclusive), e.g. (0, 100) or (100, 200)
COINS_RANK_RANGE = (1, 200)

# Force-include these base symbols no matter their rank (e.g. ["WIF","BONK","PEPE"])
EXTRA_BASE_SYMBOLS = []

# Pair quotes
QUOTE_CURRENCIES = ["USDT", "USDC"]

# Exchanges (must match modules under markets/)
MARKETS_TO_USE = [
    "binance", "kucoin", "htx", "gate", "okx", "kraken",
    "coinbase", "bitstamp", "bitfinex", "bitget", "bybit", "lbank"
]

# UI / behavior
UI_REFRESH_MS = 100               # screen refresh rate (ms)
PAGE_SIZE = 25                    # rows per page
STALE_SECS = 10 * 60              # 10 minutes
LONG_SECS = 3 * 60                # 3 minutes

# Enter/exit thresholds in PERCENT (0.6 means 0.6%)
THRESH_ENTER_PCT = 0.4
THRESH_EXIT_PCT  = 0.3

# Ignore crazy spreads (likely bad quotes)
MAX_PROFIT_PCT = 10.0

# Price formatting
MAX_DECIMALS = 12

# =========================
# Universe refreshing
# =========================
# Master switch:
# - True  => fetch /v1/coins at startup and then every UNIVERSE_REFRESH_HOURS
# - False => NEVER fetch; only read the existing file
REFRESH_UNIVERSE_ENABLED = False

# Auto-refresh interval (hours) when enabled
UNIVERSE_REFRESH_HOURS = 24

# Network timeout (seconds) for CoinPaprika calls
COINPAPRIKA_TIMEOUT = 60
