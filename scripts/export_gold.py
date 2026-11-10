"""Export materialized Gold tables directly to S3 after write approval."""
import argparse
import logging
from pathlib import Path

import boto3
import duckdb

if __package__:
    from .export_silver import DB_PATH, SOURCE_MONTH, silver_exists, silver_connection
else:
    from export_silver import DB_PATH, SOURCE_MONTH, silver_exists, silver_connection

logger = logging.getLogger(__name__)
# Deliberate allowlist: identifiers never come from CLI input.
MODELS = {
    'fct_trips': [
        ('vendor_id', 'INTEGER'), ('pickup_datetime', 'TIMESTAMP'),
        ('dropoff_datetime', 'TIMESTAMP'), ('pickup_date', 'DATE'),
        ('pickup_location_id', 'INTEGER'), ('dropoff_location_id', 'INTEGER'),
        ('passenger_count', 'BIGINT'), ('payment_type', 'BIGINT'),
        ('trip_distance', 'DOUBLE'), ('trip_duration_minutes', 'DOUBLE'),
        ('trip_amount', 'DECIMAL(18,2)'),
    ],
    'agg_daily_trips': [
        ('pickup_date', 'DATE'), ('total_trips', 'BIGINT'),
        ('total_revenue', 'DECIMAL(38,2)'), ('average_trip_amount', 'DOUBLE'),
        ('average_trip_distance', 'DOUBLE'), ('average_trip_duration', 'DOUBLE'),
    ],
    'agg_pickup_location': [
        ('pickup_location_id', 'INTEGER'), ('total_trips', 'BIGINT'),
        ('total_revenue', 'DECIMAL(38,2)'), ('average_trip_amount', 'DOUBLE'),
    ],
}


def key_for(model):
    return f'{model}/source_month={SOURCE_MONTH}/{model}.parquet'


def check_model(con, model):
    schema = [(r[0], r[1]) for r in con.execute(f'DESCRIBE {model}').fetchall()]
    rows = con.execute(f'SELECT count(*) FROM {model}').fetchone()[0]
    if schema != MODELS[model] or rows == 0:
        raise ValueError(f'{model}: empty table or unexpected schema: {schema}')
    return rows


def export_model(con, model, destination, rows):
    copied = con.execute(
        f'COPY {model} TO ? (FORMAT PARQUET, COMPRESSION ZSTD)', [destination]
    ).fetchone()[0]
    actual = con.execute('SELECT count(*) FROM read_parquet(?)', [destination]).fetchone()[0]
    if copied != rows or actual != rows:
        raise ValueError(f'{model}: row count mismatch: expected {rows}, copied {copied}, read {actual}')
    logger.info('%s: VERIFIED rows=%s destination=%s', model, rows, destination)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db-path', type=Path, default=DB_PATH)
    parser.add_argument('--upload', action='store_true', help='Requires AWS write approval')
    parser.add_argument('--single-writer', action='store_true',
                        help='Confirm exclusive access to destination keys during export')
    parser.add_argument('--bucket', required=True, help='Verified existing Gold bucket')
    args = parser.parse_args(argv)
    if not args.upload or not args.single_writer:
        parser.error('Direct export requires --upload --single-writer after approval')
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    # Same AWS_PROFILE/config credentials as DuckDB.
    s3 = boto3.client('s3')
    s3.head_bucket(Bucket=args.bucket)
    pending = []
    for model in MODELS:
        if silver_exists(s3, args.bucket, key_for(model)):
            logger.info('%s: SKIP existing s3://%s/%s', model, args.bucket, key_for(model))
        else:
            pending.append(model)
    if not pending:
        return
    try:
        with silver_connection(args.db_path) as con:
            counts = {model: check_model(con, model) for model in pending}
            for model in pending:
                export_model(con, model, f's3://{args.bucket}/{key_for(model)}', counts[model])
    except (duckdb.Error, ValueError) as exc:
        raise RuntimeError('Gold COPY/validation failed; check source, extensions, AWS profile '
                           'and permissions. Outputs may exist; inspect before rerunning.') from exc


if __name__ == '__main__':
    main()
