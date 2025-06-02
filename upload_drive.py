# upload_drive.py

import os
import json
import sys
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# 1) ENV VARS (set in GitHub Actions)
#    - GDRIVE_SA_JSON: entire service-account JSON
#    - GDRIVE_FOLDER_ID: the Drive folder ID where we want the CSV

def get_drive_service():
    sa_json_str = os.environ.get("GDRIVE_SA_JSON", None)
    if not sa_json_str:
        print("Error: GDRIVE_SA_JSON not set.", file=sys.stderr)
        sys.exit(1)
    sa_info = json.loads(sa_json_str)
    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    service = build("drive", "v3", credentials=creds, cache_discovery=False)
    return service

def find_existing_file(service, name, folder_id):
    """
    Searches for a file with the exact 'name' inside the given folder_id.
    Returns the file ID if found, or None if not found.
    """
    query = (
        f"name = '{name}' "
        f"and '{folder_id}' in parents "
        f"and trashed = false"
    )
    results = service.files().list(
        q=query,
        spaces="drive",
        fields="files(id, name)",
        pageSize=1
    ).execute()
    items = results.get("files", [])
    return items[0]["id"] if items else None

def upload_csv(local_path, drive_service, folder_id):
    """
    Upload (create or update) the file orderbook_snapshots.csv in Google Drive.
    """
    filename = os.path.basename(local_path)
    existing_id = find_existing_file(drive_service, filename, folder_id)

    media = MediaFileUpload(local_path, mimetype="text/csv", resumable=True)
    if existing_id:
        # Update the existing file
        drive_service.files().update(
            fileId=existing_id,
            media_body=media
        ).execute()
        print(f"Updated Drive file ID: {existing_id}")
    else:
        # Create a new file in the folder
        file_metadata = {
            "name": filename,
            "parents": [folder_id]
        }
        new_file = drive_service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id"
        ).execute()
        print(f"Created new Drive file ID: {new_file.get('id')}")

def main():
    local_csv = "orderbook_snapshots.csv"
    if not os.path.isfile(local_csv):
        print(f"Error: Local CSV '{local_csv}' not found.", file=sys.stderr)
        sys.exit(1)

    folder_id = os.environ.get("GDRIVE_FOLDER_ID", None)
    if not folder_id:
        print("Error: GDRIVE_FOLDER_ID not set.", file=sys.stderr)
        sys.exit(1)

    service = get_drive_service()
    upload_csv(local_csv, service, folder_id)

if __name__ == "__main__":
    main()
