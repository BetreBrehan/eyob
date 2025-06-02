# multi_poll_orderbook.py

import os
import csv
from datetime import datetime
import ccxt

# 1) CONFIGURE YOUR EXCHANGE & SYMBOLS HERE
EXCHANGE_ID = "binance"  # can be "kraken", "bybit", etc., as long as CCXT supports it

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

# 2) LOCAL CSV PATH
CSV_FILENAME = "orderbook_snapshots.csv"

def fetch_best_bid_ask(exchange, symbol):
    """
    Fetch top-of-book (best bid & ask) for a given symbol.
    Returns (bid_price, ask_price) as floats, or (None, None) on failure.
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
    Append a single row to the CSV. If the CSV doesn't exist, create it with a header first.
    - timestamp: ISO string
    - data_cols: list of (bid, ask) pairs ordered exactly like SYMBOLS list.
    """
    header = ["timestamp_utc"]
    for sym in SYMBOLS:
        col_base = sym.replace("/", "_")
        header += [f"{col_base}_bid", f"{col_base}_ask"]

    # Flatten data_cols into [bid1, ask1, bid2, ask2, ...]
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
    # 1) Initialize the exchange
    exchange_cls = getattr(ccxt, EXCHANGE_ID)
    exchange = exchange_cls({"enableRateLimit": True})
    exchange.load_markets()

    # 2) Fetch timestamp + best bids/asks for each symbol
    now_utc = datetime.utcnow().isoformat()
    results = []
    for sym in SYMBOLS:
        bid, ask = fetch_best_bid_ask(exchange, sym)
        results.append((bid, ask))

    # 3) Append to CSV
    append_row_to_csv(now_utc, results)

if __name__ == "__main__":
    main()
