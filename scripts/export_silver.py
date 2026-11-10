import argparse
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import boto3
import duckdb
from botocore.exceptions import ClientError

SOURCE_MONTH = "2025-01"
SILVER_BUCKET = "nyc-taxi-platform-silver"
EXPECTED_COLUMN_COUNT = 25

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data/warehouse/nyc_taxi.duckdb"

Schema = list[tuple[str, str]]
logger = logging.getLogger(__name__)


@contextmanager
def silver_connection(db_path: Path = DB_PATH) -> Iterator[duckdb.DuckDBPyConnection]:
    logger.info("Connecting to DuckDB: %s", db_path)
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        con.execute("LOAD httpfs")
        # Temporary secrets must be created for each connection.
        # The config provider uses the profile selected by AWS_PROFILE.
        con.execute("""
            CREATE OR REPLACE TEMP SECRET nyc_s3 (
                TYPE s3,
                PROVIDER credential_chain,
                CHAIN 'config'
            )
        """)
        yield con
    finally:
        con.close()


def check_silver_view(
    con: duckdb.DuckDBPyConnection, source_month: str
) -> tuple[int, Schema]:
    schema = [
        (row[0], row[1])
        for row in con.execute("DESCRIBE silver_yellow_trips").fetchall()
    ]
    if len(schema) != EXPECTED_COLUMN_COUNT:
        raise ValueError(
            f"Invalid Silver column count: expected {EXPECTED_COLUMN_COUNT}, "
            f"got {len(schema)}"
        )

    row_count, invalid_lineage = con.execute(
        """
        SELECT COUNT(*), COUNT(*) FILTER (
            WHERE source_month IS DISTINCT FROM ?
               OR source_file IS DISTINCT FROM ?
        )
        FROM silver_yellow_trips
        """,
        [source_month, f"yellow_tripdata_{source_month}.parquet"],
    ).fetchone()
    if row_count == 0:
        raise ValueError("Silver VIEW is empty")
    if invalid_lineage:
        raise ValueError(f"Silver VIEW has {invalid_lineage} rows with invalid lineage")

    logger.info("Silver VIEW rows: %s", row_count)
    return row_count, schema


def export_parquet(con, destination, rows):
    copied = con.execute(
        'COPY silver_yellow_trips TO ? (FORMAT PARQUET, COMPRESSION ZSTD)',
        [destination],
    ).fetchone()[0]
    actual = con.execute('SELECT count(*) FROM read_parquet(?)', [destination]).fetchone()[0]
    if copied != rows or actual != rows:
        raise ValueError(f'Silver row count mismatch: expected {rows}, copied {copied}, read {actual}')


def silver_exists(s3: Any, bucket: str, final_key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=final_key)
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        code = exc.response["Error"]["Code"]
        raise RuntimeError(
            f"Cannot check s3://{bucket}/{final_key} (AWS {code}). "
            "Verify the selected AWS profile and S3 read permissions; no export attempted."
        ) from exc
    return True



def main(argv=None):
    parser = argparse.ArgumentParser(description='Export Silver directly to S3.')
    parser.add_argument('--db-path', type=Path, default=DB_PATH)
    parser.add_argument('--upload', action='store_true', help='Requires AWS write approval')
    parser.add_argument('--single-writer', action='store_true',
                        help='Confirm exclusive access to destination keys during export')
    args = parser.parse_args(argv)
    if not args.upload or not args.single_writer:
        parser.error('Direct export requires --upload --single-writer after approval')
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    key = f'trips/source_month={SOURCE_MONTH}/yellow_trips.parquet'
    destination = f's3://{SILVER_BUCKET}/{key}'
    if silver_exists(boto3.client('s3'), SILVER_BUCKET, key):
        logger.info('SKIP existing %s', destination)
        return
    try:
        with silver_connection(args.db_path) as con:
            rows, _ = check_silver_view(con, SOURCE_MONTH)
            export_parquet(con, destination, rows)
    except (duckdb.Error, ValueError) as exc:
        raise RuntimeError(f'Silver COPY/validation failed for {destination}; check source, '
                           'extensions, AWS profile and permissions. Output may exist; '
                           'inspect before rerunning.') from exc
    logger.info('VERIFIED rows=%s destination=%s', rows, destination)


if __name__ == '__main__':
    main()
