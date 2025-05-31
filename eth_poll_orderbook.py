# eth_poll_orderbook.py

import ccxt
import csv
import os
from datetime import datetime

# ── CONFIGURATION ──
BASE_TOKEN  = "ETH"
QUOTE_TOKEN = "USD"   # Kraken’s USD market
PAIR        = f"{BASE_TOKEN}/{QUOTE_TOKEN}"

# We'll keep the CSV in the same folder as this script, i.e. the repo root when run under Actions.
CSV_PATH    = os.path.join(os.getcwd(), "orderbook_snapshots.csv")

def main():
    # Initialize CCXT (Kraken), with rate limiting
    exchange = ccxt.kraken({"enableRateLimit": True})
    exchange.load_markets()

    # If the CSV doesn't exist, create it with a header row
    if not os.path.isfile(CSV_PATH):
        with open(CSV_PATH, mode="w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp_utc", "best_bid", "best_ask"])

    # Fetch top-of-book (limit=1)
    ob = exchange.fetch_order_book(PAIR, limit=1)
    ts = datetime.utcnow().isoformat()

    if ob["bids"] and ob["asks"]:
        best_bid = float(ob["bids"][0][0])
        best_ask = float(ob["asks"][0][0])

        # Append one row to the CSV
        with open(CSV_PATH, mode="a", newline="") as f_out:
            writer = csv.writer(f_out)
            writer.writerow([ts, best_bid, best_ask])
        print(f"{ts} → bid {best_bid:.2f}, ask {best_ask:.2f}")

    else:
        print(f"{ts} → No bids/asks returned; nothing appended.")

if __name__ == "__main__":
    main()
