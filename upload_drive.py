# upload_drive.py

import os
import io
import json
import sys
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

#
# CONFIGURATION: Set these env vars in GitHub Actions
#
# - GDRIVE_SA_JSON : the full service account JSON (as a single multi-line secret)
# - GDRIVE_FOLDER_ID : the ID of the Drive folder where you want orderbook_snapshots.csv
# You can also hard‑code GDRIVE_FOLDER_ID here, but using an env var is easier.
#

def get_drive_service():
    # Load service account JSON from the environment
    sa_json_str = os.environ.get("GDRIVE_SA_JSON", None)
    if not sa_json_str:
        print("Error: GDRIVE_SA_JSON not set in environment", file=sys.stderr)
        sys.exit(1)

    # Parse it and create credentials
    sa_info = json.loads(sa_json_str)
    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/drive"]
    )

    # Build the Drive v3 service
    service = build("drive", "v3", credentials=creds, cache_discovery=False)
    return service

def find_existing_file(service, name, folder_id):
    """
    Search for a file called `name` in the given folder.
    Returns file ID if found, else None.
    """
    query = (
        f"name = '{name}' "
        f"and '{folder_id}' in parents "
        "and trashed = false"
    )
    results = service.files().list(
        q=query,
        pageSize=1,
        fields="files(id, name)"
    ).execute()
    files = results.get("files", [])
    if files:
        return files[0]["id"]
    return None

def upload_csv_to_drive(local_path, drive_service, folder_id):
    filename = os.path.basename(local_path)

    # 1) Check if it already exists
    existing_id = find_existing_file(drive_service, filename, folder_id)

    # Prepare the media upload
    media = MediaFileUpload(local_path, mimetype="text/csv", resumable=True)

    if existing_id:
        # 2a) Update existing file
        print(f"Updating existing file ID: {existing_id}")
        updated_file = (
            drive_service.files()
            .update(
                fileId=existing_id,
                media_body=media
            )
            .execute()
        )
        print(f"Updated file: {updated_file.get('id')}")
    else:
        # 2b) Create new file
        print("Creating new file in Drive...")
        file_metadata = {
            "name": filename,
            "parents": [folder_id]
        }
        created_file = (
            drive_service.files()
            .create(
                body=file_metadata,
                media_body=media,
                fields="id"
            )
            .execute()
        )
        print(f"Created file ID: {created_file.get('id')}")

def main():
    # Path to your local CSV in the workspace
    local_csv_path = "orderbook_snapshots.csv"
    if not os.path.isfile(local_csv_path):
        print(f"Error: {local_csv_path} does not exist. Exiting.", file=sys.stderr)
        sys.exit(1)

    folder_id = os.environ.get("GDRIVE_FOLDER_ID", None)
    if not folder_id:
        print("Error: GDRIVE_FOLDER_ID not set. Exiting.", file=sys.stderr)
        sys.exit(1)

    service = get_drive_service()
    upload_csv_to_drive(local_csv_path, service, folder_id)
    print("Upload to Google Drive completed.")

if __name__ == "__main__":
    main()
