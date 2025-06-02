# multi_poll_orderbook.py

import os
import csv
import sys
from datetime import datetime
import ccxt

# ── 1) CONFIGURE EXCHANGE & SYMBOLS HERE ──
# Switch EXCHANGE_ID to "gateio" and force Spot mode if needed
EXCHANGE_ID = "gateio"
EXCHANGE_OPTS = {
    "enableRateLimit": True,
    # Gate.io’s CCXT wrapper uses Spot by default—no extra "options" required
}

# The USDT‐pairs you want to track. 
# (Gate.io supports most “X/USDT” tickers for X in your list.)
WANTED_SYMBOLS = [
    "SUI/USDT",
    "WBTC/USDT",
    "STETH/USDT",
    "TRX/USDT",
    "ADA/USDT",
    "DOGE/USDT",
    "BNB/USDT",
    "SOL/USDT",
    "XRP/USDT",
    "BTC/USDT"
]

# Path to the CSV file that this script will append to
CSV_FILENAME = "orderbook_snapshots.csv"


def init_exchange_and_filter_symbols():
    """
    1) Instantiate Gate.io
    2) load_markets() to get the full list of real Spot symbols
    3) Return only those wanted symbols that actually exist
    """
    try:
        exchange_cls = getattr(ccxt, EXCHANGE_ID)
        exchange = exchange_cls(EXCHANGE_OPTS)
        markets = exchange.load_markets()
    except Exception as e:
        print(f"[ERROR] Could not initialize {EXCHANGE_ID} or load markets: {e}", file=sys.stderr)
        sys.exit(1)

    available = set(exchange.symbols)  # e.g. {"BTC/USDT", "ETH/USDT", …}

    valid = []
    for sym in WANTED_SYMBOLS:
        if sym in available:
            valid.append(sym)
        else:
            # Log any symbol that Gate.io does not actually list
            print(f"[SKIP] {sym} is not listed on Gate.io Spot → skipping", file=sys.stderr)

    if not valid:
        print("[ERROR] None of the requested symbols are on Gate.io Spot. Exiting.", file=sys.stderr)
        sys.exit(1)

    print(f"[INFO] Tracking these symbols on Gate.io Spot: {valid}", file=sys.stderr)
    return exchange, valid


def fetch_best_bid_ask(exchange, symbol):
    """
    Fetch the top-of-book (best bid & ask) for "symbol" from Gate.io.
    Returns (bid, ask) or (None, None) on failure.
    """
    try:
        ob = exchange.fetch_order_book(symbol, limit=1)
        bid = float(ob["bids"][0][0]) if ob["bids"] else None
        ask = float(ob["asks"][0][0]) if ob["asks"] else None
        return bid, ask
    except Exception as e:
        print(f"[WARN] fetch_order_book failed for {symbol}: {e}", file=sys.stderr)
        return None, None


def append_row_to_csv(timestamp, symbols, data_cols):
    """
    Append a single row to CSV_FILENAME. If the file does not exist yet, write a header first.
    - timestamp: an ISO8601 UTC string
    - symbols: list of valid symbols (e.g. ["BTC/USDT","ADA/USDT",…])
    - data_cols: list of (bid, ask) tuples in the same order as 'symbols'
    """
    # Build header row: "timestamp_utc", then "BTC_USDT_bid","BTC_USDT_ask", "ADA_USDT_bid","ADA_USDT_ask", …
    header = ["timestamp_utc"]
    for sym in symbols:
        base = sym.replace("/", "_")  # e.g. "BTC_USDT"
        header += [f"{base}_bid", f"{base}_ask"]

    # Flatten data_cols: [(bid1,ask1),(bid2,ask2),…] → [bid1,ask1,bid2,ask2,…]
    flattened = []
    for bid, ask in data_cols:
        # Use empty string for None, so the CSV cell is empty
        flattened.append(bid if bid is not None else "")
        flattened.append(ask if ask is not None else "")

    row = [timestamp] + flattened

    file_exists = os.path.isfile(CSV_FILENAME)
    with open(CSV_FILENAME, "a", newline="") as fp:
        writer = csv.writer(fp)
        if not file_exists:
            writer.writerow(header)
        writer.writerow(row)


def main():
    # 1) Initialize Gate.io and figure out which of our wanted symbols are actually listed
    exchange, valid_symbols = init_exchange_and_filter_symbols()

    # 2) Get current UTC timestamp as ISO string
    now_utc = datetime.utcnow().isoformat()

    # 3) Fetch (bid, ask) for each valid symbol
    results = []
    for s in valid_symbols:
        bid, ask = fetch_best_bid_ask(exchange, s)
        results.append((bid, ask))

    # 4) Append a new row of [timestamp, bid1, ask1, bid2, ask2, …] to the CSV
    append_row_to_csv(now_utc, valid_symbols, results)


if __name__ == "__main__":
    main()
