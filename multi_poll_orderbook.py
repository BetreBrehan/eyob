# multi_poll_orderbook.py

import os
import csv
import sys
from datetime import datetime
import ccxt

# ── 1) CONFIGURE EXCHANGE & SYMBOLS HERE ──

# Use Bybit's Spot market (not the default perpetual/swap)
EXCHANGE_ID = "bybit"
EXCHANGE_OPTS = {
    "enableRateLimit": True,
    "options": {
        "defaultType": "spot"  # <-- ensure Bybit Spot endpoints are used
    }
}

# The list of desired USDT‐pairs.
# Note: Any symbol not actually listed on Bybit Spot will be skipped (logged).
WANTED_SYMBOLS = [
    "SUI/USDT",   # Sui
    "WBTC/USDT",  # Wrapped BTC
    "STETH/USDT", # <– Does NOT exist on Bybit Spot; will be skipped
    "TRX/USDT",   # Tron
    "ADA/USDT",   # Cardano
    "DOGE/USDT",  # Dogecoin
    "BNB/USDT",   # BNB
    "SOL/USDT",   # Solana
    "XRP/USDT",   # XRP
    "BTC/USDT"    # Bitcoin
]

# Path to your local CSV file
CSV_FILENAME = "orderbook_snapshots.csv"


def init_exchange():
    """
    Instantiate the Bybit (Spot) exchange and load markets.
    Returns an exchange object and a set of available symbols.
    """
    exchange_cls = getattr(ccxt, EXCHANGE_ID)
    exchange = exchange_cls(EXCHANGE_OPTS)
    try:
        markets = exchange.load_markets()
    except Exception as e:
        print(f"[ERROR] failed load_markets(): {e}", file=sys.stderr)
        sys.exit(1)
    # exchange.symbols is a list of all valid spot symbols, e.g. ["BTC/USDT", "ETH/USDT", ...]
    return exchange, set(exchange.symbols)


def fetch_best_bid_ask(exchange, symbol):
    """
    Fetch top‐of‐book (best bid & ask) for a given symbol.
    Returns (bid, ask) as floats, or (None, None) on failure.
    """
    try:
        ob = exchange.fetch_order_book(symbol, limit=1)
        bid = float(ob["bids"][0][0]) if ob["bids"] else None
        ask = float(ob["asks"][0][0]) if ob["asks"] else None
        return bid, ask
    except Exception as e:
        # Print a warning and return (None, None)
        print(f"[WARN] fetch_order_book failed for {symbol}: {e}", file=sys.stderr)
        return None, None


def append_row_to_csv(timestamp, symbols, data_cols):
    """
    Append a single row to CSV. If the CSV does not exist, create it with a header.
    - timestamp: ISO‐formatted UTC timestamp
    - symbols: list of symbols (only the valid ones)
    - data_cols: list of (bid, ask) tuples in the same order as `symbols`
    """
    # Build header: timestamp + for each symbol, "SYMBOL_bid", "SYMBOL_ask"
    header = ["timestamp_utc"]
    for sym in symbols:
        col_base = sym.replace("/", "_")
        header += [f"{col_base}_bid", f"{col_base}_ask"]

    # Flatten the data_cols [(bid1, ask1), (bid2, ask2), …] → [bid1, ask1, bid2, ask2, …]
    flattened = []
    for bid, ask in data_cols:
        flattened += [bid if bid is not None else "", ask if ask is not None else ""]

    row = [timestamp] + flattened
    file_exists = os.path.isfile(CSV_FILENAME)
    with open(CSV_FILENAME, "a", newline="") as fp:
        writer = csv.writer(fp)
        if not file_exists:
            writer.writerow(header)
        writer.writerow(row)


def main():
    # 1) Initialize Bybit Spot and get the set of all available symbols
    exchange, available = init_exchange()

    # 2) Filter WANTED_SYMBOLS down to only those that actually exist
    valid_symbols = []
    for s in WANTED_SYMBOLS:
        if s in available:
            valid_symbols.append(s)
        else:
            print(f"[SKIP] {s} is not listed on Bybit Spot; skipping.", file=sys.stderr)

    if not valid_symbols:
        print("[ERROR] none of the desired symbols are available on Bybit Spot.", file=sys.stderr)
        sys.exit(1)

    # 3) Fetch UTC timestamp
    now_utc = datetime.utcnow().isoformat()

    # 4) For each valid symbol, get best bid/ask
    results = []
    for sym in valid_symbols:
        bid, ask = fetch_best_bid_ask(exchange, sym)
        results.append((bid, ask))

    # 5) Append one new row to the CSV
    append_row_to_csv(now_utc, valid_symbols, results)


if __name__ == "__main__":
    main()
