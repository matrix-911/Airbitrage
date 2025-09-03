import json
import requests
from typing import List, Dict

BASE = "https://api.coinpaprika.com/v1"

def fetch_all_coins(timeout: int = 60) -> List[Dict]:
    """Fetch the full coin list from CoinPaprika /v1/coins."""
    url = f"{BASE}/coins"
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()

def write_full_universe(path: str, timeout: int = 60) -> int:
    """
    Fetch ALL coins and write them directly to coins_universe.json (raw dump).
    We do not filter here; filtering happens in main at runtime.
    Returns the number of entries written.
    """
    data = fetch_all_coins(timeout=timeout)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"[get_all_coins] Saved {len(data)} coins -> {path}")
    return len(data)

def main():
    # CLI usage: writes all coins to the default file name
    write_full_universe(path="coins_universe.json", timeout=60)

if __name__ == "__main__":
    main()
