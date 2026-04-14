import boto3
import os
from pathlib import Path

R2_ACCOUNT_ID    = os.environ["R2_ACCOUNT_ID"]
R2_ACCESS_KEY    = os.environ["R2_ACCESS_KEY"]
R2_SECRET_KEY    = os.environ["R2_SECRET_KEY"]
R2_BUCKET        = os.environ.get("R2_BUCKET", "qrl-risk-data")
LOCAL_DATA_DIR   = Path(os.environ.get("QRL_BASE_DIR", Path(__file__).resolve().parent.parent)) / "data"

s3 = boto3.client(
    "s3",
    endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
    region_name="auto",
)

def upload_directory(local_dir: Path, prefix: str = "data"):
    files = list(local_dir.rglob("*.parquet"))
    print(f"Found {len(files)} parquet files to upload.")
    for i, file_path in enumerate(files, 1):
        relative = file_path.relative_to(local_dir.parent)
        key = str(relative).replace("\\", "/")
        print(f"[{i}/{len(files)}] Uploading {key}...")
        s3.upload_file(str(file_path), R2_BUCKET, key)
    print("Upload complete.")

if __name__ == "__main__":
    upload_directory(LOCAL_DATA_DIR)
