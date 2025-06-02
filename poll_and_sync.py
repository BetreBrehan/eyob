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

# 1.a) Google Drive setup
#   The GDRIVE_SA_JSON and GDRIVE_FOLDER_ID environment variables must be set
#   by GitHub Actions via secrets. Do NOT hard‑code your JSON or folder ID here.
#
# GDRIVE_SA_JSON should be the entire service-account JSON string (exactly as downloaded).
# GDRIVE_FOLDER_ID should be the ID of the Drive folder where you want to store the CSV.

SA_JSON_ENVVAR = "GDRIVE_SA_JSON"
FOLDER_ID_ENVVAR = "GDRIVE_FOLDER_ID"
REMOTE_FILENAME = "orderbook_snapshots.csv"  # the exact name we use on Drive


# 1.b) Exchange & symbols
EXCHANGE_ID = "gateio"
EXCHANGE_OPTS = {
    "enableRateLimit": True,
    # Gate.io defaults to Spot, so no extra "options" needed
}

# List all the USDT pairs you wish to track. On Gate.io Spot, some of these
# may exist and some may not. We will skip any that Gate.io does not list.
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

# Local temporary path (within the GitHub runner) for downloading the CSV,
# appending a row, and then re‑uploading. "/mnt/data" is writeable.
LOCAL_CSV_PATH = "/mnt/data/orderbook_snapshots.csv"


# ── 2) DRIVE HELPERS ────────────────────────────────────────────────────────────

def get_drive_service():
    """
    Create and return a Google Drive API service using the service-account JSON
    stored in the GDRIVE_SA_JSON environment variable.
    """
    sa_json_str = os.environ.get(SA_JSON_ENVVAR, None)
    if not sa_json_str:
        print(f"[ERROR] Env var {SA_JSON_ENVVAR} not set", file=sys.stderr)
        sys.exit(1)

    sa_info = None
    try:
        sa_info = service_account.Credentials.from_service_account_info(
            __import__("json").loads(sa_json_str),
            scopes=["https://www.googleapis.com/auth/drive"],
        )
    except Exception as e:
        print(f"[ERROR] Failed to parse service account JSON: {e}", file=sys.stderr)
        sys.exit(1)

    drive_service = build("drive", "v3", credentials=sa_info, cache_discovery=False)
    return drive_service


def find_remote_file_id(service, filename, folder_id):
    """
    Search for 'filename' in the given Drive folder (folder_id).
    Returns the file ID if found, or None if not found.
    """
    query = (
        f"name = '{filename}' "
        f"and '{folder_id}' in parents "
        "and trashed = false"
    )
    resp = service.files().list(q=query, fields="files(id, name)", pageSize=1).execute()
    files = resp.get("files", [])
    if files:
        return files[0]["id"]
    return None


def download_remote_csv(service, file_id, local_path):
    """
    Download the remote file (file_id) from Drive into local_path.
    Overwrites local_path if it exists.
    """
    fh = io.FileIO(local_path, mode="wb")
    downloader = MediaIoBaseDownload(fh, service.files().get_media(fileId=file_id))
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.close()


def upload_csv_to_drive(service, local_path, folder_id, existing_file_id):
    """
    If existing_file_id is provided, do an update (overwrite).
    Otherwise, create a new file in the folder.
    """
    media = MediaFileUpload(local_path, mimetype="text/csv", resumable=True)
    if existing_file_id:
        service.files().update(
            fileId=existing_file_id,
            media_body=media
        ).execute()
    else:
        metadata = {
            "name": REMOTE_FILENAME,
            "parents": [folder_id]
        }
        service.files().create(
            body=metadata, 
            media_body=media,
            fields="id"
        ).execute()


# ── 3) EXCHANGE POLLING HELPERS ─────────────────────────────────────────────────

def init_exchange_and_filter_symbols():
    """
    1) Instantiate Gate.io Spot and call load_markets()
    2) Filter WANTED_SYMBOLS to only those actually listed on Gate.io
    3) Return (exchange, valid_symbols_list)
    """
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
    """
    Fetch the top‐of‐book for a given symbol (e.g. "BTC/USDT") from Gate.io.
    Returns (bid, ask) as floats, or (None, None) on any failure.
    """
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
    """
    Append a timestamp + bids/asks for each symbol to the CSV at local_path.
    If the file does not exist yet, write a header first.
    - symbols: list of valid symbol strings (e.g. ["BTC/USDT","ADA/USDT", …])
    - data_cols: list of (bid, ask) tuples in the same order as 'symbols'
    """
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
    # 1) Verify Drive environment variables
    folder_id = os.environ.get(FOLDER_ID_ENVVAR, None)
    if not folder_id:
        print(f"[ERROR] Env var {FOLDER_ID_ENVVAR} not set", file=sys.stderr)
        sys.exit(1)

    # 2) Build the Drive API client
    drive_service = get_drive_service()

    # 3) Look for existing file on Drive
    remote_id = find_remote_file_id(drive_service, REMOTE_FILENAME, folder_id)

    # 4) If it exists, download it locally so we can append to it
    if remote_id:
        try:
            download_remote_csv(drive_service, remote_id, LOCAL_CSV_PATH)
        except Exception as e:
            print(f"[ERROR] Could not download existing CSV from Drive: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        # If the file didn’t exist on Drive, ensure any local copy is removed so
        # we create a brand‑new CSV with a header below.
        if os.path.isfile(LOCAL_CSV_PATH):
            os.remove(LOCAL_CSV_PATH)

    # 5) Initialize Gate.io Spot and filter symbols
    exchange, valid_symbols = init_exchange_and_filter_symbols()

    # 6) Fetch best bid/ask for each valid symbol
    data_cols = []
    for sym in valid_symbols:
        bid, ask = fetch_best_bid_ask(exchange, sym)
        data_cols.append((bid, ask))

    # 7) Append one new row to the local CSV
    append_row_to_local_csv(LOCAL_CSV_PATH, valid_symbols, data_cols)

    # 8) Upload the updated CSV back to Drive (create or update)
    try:
        upload_csv_to_drive(drive_service, LOCAL_CSV_PATH, folder_id, remote_id)
        print("[INFO] Uploaded CSV to Google Drive successfully.")
    except Exception as e:
        print(f"[ERROR] Failed to upload CSV to Drive: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
