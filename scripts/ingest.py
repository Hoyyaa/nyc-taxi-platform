import hashlib
import logging
from pathlib import Path
from typing import Literal

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATASET = "yellow"
YEAR = 2025
MONTH = 1
BUCKET_NAME = "nyc-taxi-platform-bronze"

FILE_NAME = f"{DATASET}_tripdata_{YEAR}-{MONTH:02d}.parquet"
LOCAL_FILE = PROJECT_ROOT / "data" / "raw" / FILE_NAME

AWS_PROFILE = "nyc-taxi"
AWS_REGION = "ap-southeast-1"

UploadStatus = Literal["UPLOAD", "SKIP", "FAIL"]


def build_object_key(dataset: str, year: int, month: int) -> str:
    filename = f"{dataset}_tripdata_{year}-{month:02d}.parquet"

    return f"{dataset}/year={year}/month={month:02d}/{filename}"


def calculate_sha256(file_path: Path) -> str:
    sha256 = hashlib.sha256()

    with file_path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            sha256.update(chunk)

    return sha256.hexdigest()


def check_upload_status(s3, bucket: str, key: str, checksum: str) -> UploadStatus:
    """Decide whether to upload, skip, or reject an existing object."""
    try:
        response = s3.head_object(Bucket=bucket, Key=key)

    except ClientError as error:
        error_code = error.response["Error"]["Code"]

        if error_code in ("404", "NoSuchKey", "NotFound"):
            return "UPLOAD"

        logger.error(
            "S3 head_object failed. Bucket=%s, Key=%s, Error=%s",
            bucket,
            key,
            error_code,
        )
        raise

    remote_checksum = response.get("Metadata", {}).get("sha256")

    if remote_checksum == checksum:
        return "SKIP"

    return "FAIL"


def main() -> None:
    """Upload the local file, skipping objects with matching SHA-256 metadata."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s - %(message)s",
    )

    if not LOCAL_FILE.is_file():
        raise FileNotFoundError(f"File not found: {LOCAL_FILE}")

    object_key = build_object_key(DATASET, YEAR, MONTH)

    logger.info("Local file: %s", LOCAL_FILE)
    logger.info("File size: %s bytes", f"{LOCAL_FILE.stat().st_size:,}")
    logger.info("S3 destination: s3://%s/%s", BUCKET_NAME, object_key)

    session = boto3.Session(
        profile_name=AWS_PROFILE,
        region_name=AWS_REGION,
    )
    s3 = session.client("s3")

    checksum = calculate_sha256(LOCAL_FILE)
    logger.info("SHA-256: %s", checksum)

    status = check_upload_status(
        s3=s3,
        bucket=BUCKET_NAME,
        key=object_key,
        checksum=checksum,
    )

    logger.info("Ingestion decision: %s", status)
    if status == "SKIP":
        logger.info("File already exists with matching checksum. Skipping.")
        return

    if status == "FAIL":
        raise RuntimeError(
            "Upload aborted: S3 object has different or missing SHA-256 metadata."
        )

    logger.info("Starting S3 upload...")
    s3.upload_file(
        Filename=str(LOCAL_FILE),
        Bucket=BUCKET_NAME,
        Key=object_key,
        ExtraArgs={"Metadata": {"sha256": checksum}},
    )
    logger.info("S3 upload completed successfully")


if __name__ == "__main__":
    main()
