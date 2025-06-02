# multi_poll_orderbook.py

import os
import csv
from datetime import datetime
import ccxt

# ── 1) CONFIGURE EXCHANGE & SYMBOLS HERE ──
# Switch from Binance (geo‑restricted) to Bybit (open globally)
EXCHANGE_ID = "bybit"

# The list of USDT‐pairs you requested (add more if needed)
SYMBOLS = [
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

# ── 2) LOCAL CSV FILENAME ──
CSV_FILENAME = "orderbook_snapshots.csv"

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
    except Exception:
        return None, None

def append_row_to_csv(timestamp, data_cols):
    """
    Append a row to CSV. If CSV does not exist, write header first.
    - timestamp: ISO string
    - data_cols: list of (bid, ask) pairs in the same order as SYMBOLS
    """
    # Build header: timestamp + each SYMBOL_bid / SYMBOL_ask
    header = ["timestamp_utc"]
    for sym in SYMBOLS:
        base = sym.replace("/", "_")
        header += [f"{base}_bid", f"{base}_ask"]

    # Flatten [(bid1,ask1),(bid2,ask2),…] into [bid1,ask1,bid2,ask2,…]
    flattened = []
    for bid, ask in data_cols:
        # Use empty string if None
        flattened += [bid if bid is not None else "", ask if ask is not None else ""]

    row = [timestamp] + flattened

    file_exists = os.path.isfile(CSV_FILENAME)
    with open(CSV_FILENAME, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(header)
        writer.writerow(row)

def main():
    # Initialize the Bybit exchange (no API key needed for public order‐book)
    exchange_cls = getattr(ccxt, EXCHANGE_ID)
    exchange = exchange_cls({"enableRateLimit": True})
    try:
        exchange.load_markets()
    except Exception:
        # If load_markets fails (rare, but just skip it),
        # we can still call fetch_order_book directly on known symbols.
        pass

    # Fetch the current UTC timestamp
    now_utc = datetime.utcnow().isoformat()

    # Fetch best‐bid/best‐ask for each symbol
    results = []
    for sym in SYMBOLS:
        bid, ask = fetch_best_bid_ask(exchange, sym)
        results.append((bid, ask))

    # Append this row to the CSV
    append_row_to_csv(now_utc, results)

if __name__ == "__main__":
    main()
