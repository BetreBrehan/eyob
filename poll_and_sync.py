# poll_and_sync.py

import os
import io
import sys
from datetime import datetime
import ccxt
import pickle

from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

# ── CONFIG ──────────────────────────────────────────────────────────────────────
SCOPES = ['https://www.googleapis.com/auth/drive.file']
FOLDER_ID_ENVVAR = "GDRIVE_FOLDER_ID"
REMOTE_FILENAME = "orderbook_snapshots.csv"
LOCAL_CSV_PATH = "orderbook_snapshots.csv"

WANTED_SYMBOLS = [
    "SUI/USDT","WBTC/USDT","STETH/USDT","TRX/USDT",
    "ADA/USDT","DOGE/USDT","BNB/USDT","SOL/USDT",
    "XRP/USDT","BTC/USDT"
]
EXCHANGE_ID = "gateio"
EXCHANGE_OPTS = {"enableRateLimit": True}

# ── DRIVE AUTH ──────────────────────────────────────────────────────────────────
def get_drive_service():
    """
    Authorize with OAuth 2.0 (user account) and return a Drive v3 service.
    Saves and reuses token.pickle for refresh.
    """
    creds = None
    token_path = 'token.pickle'

    if os.path.exists(token_path):
        with open(token_path, 'rb') as token_file:
            creds = pickle.load(token_file)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, 'wb') as token_file:
            pickle.dump(creds, token_file)

    return build('drive', 'v3', credentials=creds)

# ── DRIVE HELPERS ───────────────────────────────────────────────────────────────
def find_remote_file_id(service, filename, folder_id):
    query = f"name = '{filename}' and '{folder_id}' in parents and trashed = false"
    resp = service.files().list(q=query, fields="files(id)", pageSize=1).execute()
    files = resp.get('files', [])
    return files[0]['id'] if files else None


def download_remote_csv(service, file_id, local_path):
    with io.FileIO(local_path, mode='wb') as fh:
        downloader = MediaIoBaseDownload(fh, service.files().get_media(fileId=file_id))
        done = False
        while not done:
            _, done = downloader.next_chunk()


def upload_csv_to_drive(service, local_path, folder_id, existing_file_id):
    media = MediaFileUpload(local_path, mimetype='text/csv', resumable=True)
    if existing_file_id:
        service.files().update(fileId=existing_file_id, media_body=media).execute()
    else:
        metadata = {'name': REMOTE_FILENAME, 'parents': [folder_id]}
        service.files().create(body=metadata, media_body=media, fields='id').execute()

# ── EXCHANGE HELPERS ────────────────────────────────────────────────────────────
def init_exchange_and_filter_symbols():
    try:
        exchange = getattr(ccxt, EXCHANGE_ID)(EXCHANGE_OPTS)
        exchange.load_markets()
    except Exception as e:
        print(f"[ERROR] Init {EXCHANGE_ID}: {e}", file=sys.stderr)
        sys.exit(1)

    available = set(exchange.symbols)
    valid = [sym for sym in WANTED_SYMBOLS if sym in available]
    for sym in WANTED_SYMBOLS:
        if sym not in available:
            print(f"[SKIP] {sym} not on {EXCHANGE_ID} → skipping", file=sys.stderr)

    if not valid:
        print("[ERROR] No valid symbols found. Exiting.", file=sys.stderr)
        sys.exit(1)

    print(f"[INFO] Tracking symbols: {valid}", file=sys.stderr)
    return exchange, valid


def fetch_best_bid_ask(exchange, symbol):
    try:
        ob = exchange.fetch_order_book(symbol, limit=1)
        bid = float(ob['bids'][0][0]) if ob['bids'] else None
        ask = float(ob['asks'][0][0]) if ob['asks'] else None
        return bid, ask
    except Exception as e:
        print(f"[WARN] fetch_order_book {symbol}: {e}", file=sys.stderr)
        return None, None

# ── CSV FUNCTIONS ────────────────────────────────────────────────────────────────
def append_row_to_local_csv(local_path, symbols, data_cols):
    header_cols = ['timestamp_utc'] + [f"{sym.replace('/','_')}_{side}"
        for sym in symbols for side in ('bid','ask')]
    header = ','.join(header_cols) + '\n'
    timestamp = datetime.utcnow().isoformat()
    row = [str(x) if x is not None else '' for pair in data_cols for x in pair]
    line = timestamp + ',' + ','.join(row) + '\n'

    if not os.path.isfile(local_path):
        with open(local_path, 'w') as f:
            f.write(header)
            f.write(line)
        return

    with open(local_path, 'r') as f:
        first = f.readline().strip()
    if not first.startswith('timestamp_utc'):
        data = open(local_path).read()
        with open(local_path, 'w') as f:
            f.write(header + data + line)
    else:
        with open(local_path, 'a') as f:
            f.write(line)

# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    folder_id = os.environ.get(FOLDER_ID_ENVVAR)
    if not folder_id:
        print(f"[ERROR] Env var {FOLDER_ID_ENVVAR} not set", file=sys.stderr)
        sys.exit(1)

    drive_service = get_drive_service()
    remote_id = find_remote_file_id(drive_service, REMOTE_FILENAME, folder_id)

    if remote_id:
        try:
            download_remote_csv(drive_service, remote_id, LOCAL_CSV_PATH)
        except Exception as e:
            print(f"[ERROR] Download failed: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        if os.path.isfile(LOCAL_CSV_PATH):
            os.remove(LOCAL_CSV_PATH)

    exchange, symbols = init_exchange_and_filter_symbols()
    data = [fetch_best_bid_ask(exchange, sym) for sym in symbols]
    append_row_to_local_csv(LOCAL_CSV_PATH, symbols, data)

    try:
        upload_csv_to_drive(drive_service, LOCAL_CSV_PATH, folder_id, remote_id)
        print("[INFO] Uploaded CSV to Google Drive successfully.")
    except Exception as e:
        print(f"[ERROR] Upload failed: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
