# poll_and_sync.py

import os
import csv
import io
import sys
from datetime import datetime
import ccxt

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

# ── 1) CONFIGURATION ───────────────────────────────────────────────────────────

# Google Drive env vars (set via GitHub Secrets)
SA_JSON_ENVVAR = "GDRIVE_SA_JSON"
FOLDER_ID_ENVVAR = "GDRIVE_FOLDER_ID"
REMOTE_FILENAME = "orderbook_snapshots.csv"

# Exchange & symbols
EXCHANGE_ID = "gateio"
EXCHANGE_OPTS = {
    "enableRateLimit": True,
    # Gate.io defaults to Spot; no extra options needed
}

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

# ← CHANGE: Use a relative path (current working directory) instead of /mnt/data
LOCAL_CSV_PATH = "orderbook_snapshots.csv"


# ── 2) DRIVE HELPERS ────────────────────────────────────────────────────────────

def get_drive_service():
    sa_json_str = os.environ.get(SA_JSON_ENVVAR, None)
    if not sa_json_str:
        print(f"[ERROR] Env var {SA_JSON_ENVVAR} not set", file=sys.stderr)
        sys.exit(1)

    try:
        sa_info = service_account.Credentials.from_service_account_info(
            __import__("json").loads(sa_json_str),
            scopes=["https://www.googleapis.com/auth/drive"],
        )
    except Exception as e:
        print(f"[ERROR] Failed to parse service account JSON: {e}", file=sys.stderr)
        sys.exit(1)

    return build("drive", "v3", credentials=sa_info, cache_discovery=False)


def find_remote_file_id(service, filename, folder_id):
    query = (
        f"name = '{filename}' "
        f"and '{folder_id}' in parents "
        "and trashed = false"
    )
    resp = service.files().list(q=query, fields="files(id, name)", pageSize=1).execute()
    files = resp.get("files", [])
    return files[0]["id"] if files else None


def download_remote_csv(service, file_id, local_path):
    fh = io.FileIO(local_path, mode="wb")
    downloader = MediaIoBaseDownload(fh, service.files().get_media(fileId=file_id))
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.close()


def upload_csv_to_drive(service, local_path, folder_id, existing_file_id):
    media = MediaFileUpload(local_path, mimetype="text/csv", resumable=True)
    if existing_file_id:
        service.files().update(fileId=existing_file_id, media_body=media).execute()
    else:
        metadata = {"name": REMOTE_FILENAME, "parents": [folder_id]}
        service.files().create(body=metadata, media_body=media, fields="id").execute()


# ── 3) EXCHANGE POLLING HELPERS ─────────────────────────────────────────────────

def init_exchange_and_filter_symbols():
    try:
        ex_cls = getattr(ccxt, EXCHANGE_ID)
        exchange = ex_cls(EXCHANGE_OPTS)
        exchange.load_markets()
    except Exception as e:
        print(f"[ERROR] Could not init {EXCHANGE_ID} or load markets: {e}", file=sys.stderr)
        sys.exit(1)

    available = set(exchange.symbols)
    valid = []
    for sym in WANTED_SYMBOLS:
        if sym in available:
            valid.append(sym)
        else:
            print(f"[SKIP] {sym} is not listed on Gate.io Spot → skipping", file=sys.stderr)

    if not valid:
        print("[ERROR] None of the requested symbols are on Gate.io Spot. Exiting.", file=sys.stderr)
        sys.exit(1)

    print(f"[INFO] Valid symbols on Gate.io Spot: {valid}", file=sys.stderr)
    return exchange, valid


def fetch_best_bid_ask(exchange, symbol):
    try:
        ob = exchange.fetch_order_book(symbol, limit=1)
        bid = float(ob["bids"][0][0]) if ob["bids"] else None
        ask = float(ob["asks"][0][0]) if ob["asks"] else None
        return bid, ask
    except Exception as e:
        print(f"[WARN] fetch_order_book failed for {symbol}: {e}", file=sys.stderr)
        return None, None


# ── 4) CSV HELPERS ────────────────────────────────────────────────────────────────

def append_row_to_local_csv(local_path, symbols, data_cols):
    header = ["timestamp_utc"]
    for sym in symbols:
        base = sym.replace("/", "_")
        header += [f"{base}_bid", f"{base}_ask"]

    flattened = []
    for bid, ask in data_cols:
        flattened.append(bid if bid is not None else "")
        flattened.append(ask if ask is not None else "")

    row = [datetime.utcnow().isoformat()] + flattened

    file_exists = os.path.isfile(local_path)
    with open(local_path, "a", newline="") as fp:
        writer = csv.writer(fp)
        if not file_exists:
            writer.writerow(header)
        writer.writerow(row)


# ── 5) MAIN: DOWNLOAD → POLL → APPEND → UPLOAD ──────────────────────────────────

def main():
    folder_id = os.environ.get(FOLDER_ID_ENVVAR, None)
    if not folder_id:
        print(f"[ERROR] Env var {FOLDER_ID_ENVVAR} not set", file=sys.stderr)
        sys.exit(1)

    drive_service = get_drive_service()
    remote_id = find_remote_file_id(drive_service, REMOTE_FILENAME, folder_id)

    # If the remote CSV exists, download it to LOCAL_CSV_PATH; otherwise remove any leftover local file
    if remote_id:
        try:
            download_remote_csv(drive_service, remote_id, LOCAL_CSV_PATH)
        except Exception as e:
            print(f"[ERROR] Could not download existing CSV from Drive: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        if os.path.isfile(LOCAL_CSV_PATH):
            os.remove(LOCAL_CSV_PATH)

    exchange, valid_symbols = init_exchange_and_filter_symbols()

    data_cols = []
    for sym in valid_symbols:
        bid, ask = fetch_best_bid_ask(exchange, sym)
        data_cols.append((bid, ask))

    append_row_to_local_csv(LOCAL_CSV_PATH, valid_symbols, data_cols)

    try:
        upload_csv_to_drive(drive_service, LOCAL_CSV_PATH, folder_id, remote_id)
        print("[INFO] Uploaded CSV to Google Drive successfully.")
    except Exception as e:
        print(f"[ERROR] Failed to upload CSV to Drive: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
