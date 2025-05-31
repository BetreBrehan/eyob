# poll_orderbook.py

import ccxt
import csv
from datetime import datetime
import os

# ── CONFIGURATION ──
BASE_TOKEN  = "ETH"
QUOTE_TOKEN = "USD"
PAIR        = f"{BASE_TOKEN}/{QUOTE_TOKEN}"
# The runner’s working directory is the repo root, so this path is relative to that.
CSV_PATH    = "orderbook_snapshots.csv"

# Initialize CCXT (Kraken). You can swap “kraken” for “binance” or other exchange.
exchange = ccxt.kraken({"enableRateLimit": True})
exchange.load_markets()

# Create CSV with header if it does not exist
if not os.path.isfile(CSV_PATH):
    with open(CSV_PATH, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp_utc", "best_bid", "best_ask"])

# Fetch a single snapshot (top of book)
ob = exchange.fetch_order_book(PAIR, limit=1)
ts = datetime.utcnow().isoformat()
if ob["bids"] and ob["asks"]:
    best_bid = float(ob["bids"][0][0])
    best_ask = float(ob["asks"][0][0])
    with open(CSV_PATH, "a", newline="") as f_out:
        writer = csv.writer(f_out)
        writer.writerow([ts, best_bid, best_ask])
    print(f"{ts} → bid {best_bid:.2f}, ask {best_ask:.2f}")
else:
    print(f"{ts} → No bids/asks returned; nothing appended.")
