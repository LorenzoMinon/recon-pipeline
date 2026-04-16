"""
upload_to_s3.py
Uploads ClearVault Corp raw CSV files to S3 landing zone.
"""

import boto3
import os
from pathlib import Path

# ----- Configuration -----
BUCKET_NAME = "clearvault-raw-data"
LOCAL_DIR = "data/raw"
S3_PREFIX = "raw"  # files will land at s3://clearvault-raw-data/raw/


def upload_files():
    s3 = boto3.client("s3")

    files = list(Path(LOCAL_DIR).glob("*.csv"))
    if not files:
        print(f"No CSV files found in {LOCAL_DIR}")
        return

    print(f"Uploading {len(files)} files to s3://{BUCKET_NAME}/{S3_PREFIX}/\n")

    for file_path in files:
        s3_key = f"{S3_PREFIX}/{file_path.name}"
        print(f"  Uploading {file_path.name}...")
        s3.upload_file(str(file_path), BUCKET_NAME, s3_key)
        print(f"  Done → s3://{BUCKET_NAME}/{s3_key}")

    print(f"\nAll files uploaded successfully.")


if __name__ == "__main__":
    upload_files()