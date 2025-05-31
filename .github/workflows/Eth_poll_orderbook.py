# poll_orderbook.py
import ccxt
import csv
from datetime import datetime

# CONFIGURATION
BASE_TOKEN  = "ETH"
QUOTE_TOKEN = "USD"
PAIR        = f"{BASE_TOKEN}/{QUOTE_TOKEN}"
CSV_PATH    = "/home/yourusername/orderbook_snapshots.csv"  # PythonAnywhere home

# Initialize CCXT (Kraken)
exchange = ccxt.kraken({"enableRateLimit": True})
exchange.load_markets()

# Ensure CSV exists & has header
try:
    with open(CSV_PATH, "x", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp_utc", "best_bid", "best_ask"])
except FileExistsError:
    pass

# Fetch current order book (top 1)
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
    print(f"{ts} → no bids/asks returned")
