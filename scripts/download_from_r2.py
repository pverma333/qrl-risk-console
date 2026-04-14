import boto3
import os
from pathlib import Path

R2_ACCOUNT_ID = os.environ["R2_ACCOUNT_ID"]
R2_ACCESS_KEY = os.environ["R2_ACCESS_KEY"]
R2_SECRET_KEY = os.environ["R2_SECRET_KEY"]
R2_BUCKET     = os.environ.get("R2_BUCKET", "qrl-risk-data")
BASE_DIR      = Path(os.environ.get("QRL_BASE_DIR", "/tmp/qrl"))

s3 = boto3.client(
    "s3",
    endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
    region_name="auto",
)

def download_all():
    paginator = s3.get_paginator("list_objects_v2")
    pages     = paginator.paginate(Bucket=R2_BUCKET, Prefix="data/")
    total     = 0

    for page in pages:
        for obj in page.get("Contents", []):
            key        = obj["Key"]
            local_path = BASE_DIR / key
            local_path.parent.mkdir(parents=True, exist_ok=True)

            if local_path.exists():
                r2_size    = obj["Size"]
                local_size = local_path.stat().st_size
                if r2_size == local_size:
                    continue

            print(f"Downloading {key}...")
            s3.download_file(R2_BUCKET, key, str(local_path))
            total += 1

    print(f"Download complete. {total} files updated.")

    ingest_dir  = BASE_DIR / "data" / "ingest"
    curated_dir = BASE_DIR / "data" / "curated"

    if not ingest_dir.exists():
        raise RuntimeError(f"CRITICAL: ingest dir missing after download: {ingest_dir}")
    if not curated_dir.exists():
        raise RuntimeError(f"CRITICAL: curated dir missing after download: {curated_dir}")

    print(f"Verified ingest:  {ingest_dir}")
    print(f"Verified curated: {curated_dir}")
    print(f"Data dir contents: {[p.name for p in (BASE_DIR / 'data').iterdir()]}")

if __name__ == "__main__":
    download_all()
