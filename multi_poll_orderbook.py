# multi_poll_orderbook.py

import os
import csv
import sys
from datetime import datetime
import ccxt

# ── 1) CONFIGURE EXCHANGE & SYMBOLS HERE ──

# We MUST force Bybit to use its Spot API (not perpetual/swap).
# Otherwise, many USDT tickers don’t exist and you get None bids/asks.
EXCHANGE_ID = "bybit"
EXCHANGE_OPTS = {
    "enableRateLimit": True,
    "options": {
        "defaultType": "spot"   # <— this forces Bybit Spot
    }
}

# Here are the 10 USDT-pairs you asked for. 
# (If a symbol isn’t on Bybit Spot, it will get skipped automatically.)
WANTED_SYMBOLS = [
    "SUI/USDT",    # SUI on Bybit Spot? (should exist if Bybit listed it)
    "WBTC/USDT",   # Wrapped BTC
    "STETH/USDT",  # STETH is NOT on Bybit Spot → will be skipped
    "TRX/USDT",    # TRON
    "ADA/USDT",    # Cardano
    "DOGE/USDT",   # Dogecoin
    "BNB/USDT",    # BNB
    "SOL/USDT",    # Solana
    "XRP/USDT",    # XRP
    "BTC/USDT"     # Bitcoin
]

# Path to the local CSV (in the GitHub workspace) that we append each run
CSV_FILENAME = "orderbook_snapshots.csv"



def init_exchange_and_filter_symbols():
    """
    1) Instantiate Bybit Spot
    2) load_markets()
    3) Build a list of only those symbols from WANTED_SYMBOLS that truly exist on Bybit Spot
    4) Print to stderr which symbols were kept vs skipped, for debugging
    Returns: (exchange_obj, valid_symbols_list)
    """
    try:
        ex_cls = getattr(ccxt, EXCHANGE_ID)
        exchange = ex_cls(EXCHANGE_OPTS)
        markets = exchange.load_markets()
    except Exception as e:
        print(f"[ERROR] Could not initialize {EXCHANGE_ID} spot or load markets: {e}", file=sys.stderr)
        sys.exit(1)

    available = set(exchange.symbols)  # all symbol strings on Bybit Spot, e.g. "BTC/USDT", "ETH/USDT", etc.

    valid_symbols = []
    for sym in WANTED_SYMBOLS:
        if sym in available:
            valid_symbols.append(sym)
        else:
            # Log to stderr that this symbol does not exist on Bybit Spot
            print(f"[SKIP] Symbol {sym} is not listed on Bybit Spot → skipping it", file=sys.stderr)

    if not valid_symbols:
        print("[ERROR] None of the requested symbols exist on Bybit Spot. Exiting.", file=sys.stderr)
        sys.exit(1)

    print(f"[INFO] Valid symbols on Bybit Spot: {valid_symbols}", file=sys.stderr)
    return exchange, valid_symbols



def fetch_best_bid_ask(exchange, symbol):
    """
    Given a CCXT exchange instance and a single symbol (e.g. "BTC/USDT"),
    fetch the top-of-book order book and return (bid, ask) as floats.
    If anything goes wrong, log to stderr and return (None, None).
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
    Append a single CSV row to CSV_FILENAME. If the file doesn’t exist, write a header first.

    - timestamp: ISO-format string
    - symbols: list of the valid symbol strings (like ["BTC/USDT", "ADA/USDT", ...])
    - data_cols: list of (bid, ask) tuples in the same order as `symbols`

    The CSV’s header looks like:
      timestamp_utc,BTC_USDT_bid,BTC_USDT_ask,ADA_USDT_bid,ADA_USDT_ask,…
    and each row looks like:
      2025-06-03T12:05:00,65000.12,65000.35,0.3405,0.3407,… 
    """
    # Build header row
    header = ["timestamp_utc"]
    for sym in symbols:
        base = sym.replace("/", "_")  # e.g. "BTC/USDT" → "BTC_USDT"
        header += [f"{base}_bid", f"{base}_ask"]

    # Flatten the list of (bid,ask) pairs to [bid1, ask1, bid2, ask2, ...]
    flattened = []
    for bid, ask in data_cols:
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
    # 1) Init Bybit Spot & filter symbols
    exchange, valid_symbols = init_exchange_and_filter_symbols()

    # 2) Get current UTC timestamp
    now_utc = datetime.utcnow().isoformat()

    # 3) Fetch (bid, ask) for each valid symbol
    results = []
    for sym in valid_symbols:
        bid, ask = fetch_best_bid_ask(exchange, sym)
        results.append((bid, ask))

    # 4) Append one new row to the CSV
    append_row_to_csv(now_utc, valid_symbols, results)


if __name__ == "__main__":
    main()
