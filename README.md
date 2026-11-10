# NYC Taxi Data Platform MVP

January 2025 Yellow Taxi only:
NYC TLC → Python/boto3 → S3 Bronze → dbt/DuckDB → S3 Silver
→ dbt/DuckDB → S3 Gold.
The current Bronze script reads the downloaded TLC Parquet locally.
Docker and Airflow are deferred.

## Setup

Run from the repository root using Python 3.11+:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
export AWS_PROFILE=nyc-taxi
export AWS_DEFAULT_REGION=ap-southeast-1
export NYC_TAXI_DB_PATH="$PWD/data/warehouse/nyc_taxi.duckdb"
mkdir -p data/raw data/warehouse
```

Use an existing AWS profile with valid credentials; never put credentials in this
repository. Place `yellow_tripdata_2025-01.parquet` under `data/raw/`.
The dependencies are pinned to versions present in the development environment.
DuckDB needs its httpfs/aws extensions available (initial setup may need network).

## Run manually

Before any AWS-writing command below, show the exact destination to the project
owner and obtain approval. No AWS-writing command was run during Phase 2.

1. Bronze destination:
   `s3://nyc-taxi-platform-bronze/yellow/year=2025/month=01/yellow_tripdata_2025-01.parquet`

   After approval:
   ```bash
   python scripts/ingest.py
   ```
   Existing matching SHA-256 metadata causes a skip; missing/different metadata
   causes a conflict error. Bronze code is unchanged.

2. Create the local Silver view and test it (reads S3 Bronze):
   ```bash
   dbt run --project-dir dbt/nyc_taxi --profiles-dir dbt/nyc_taxi --select silver_yellow_trips
   dbt test --project-dir dbt/nyc_taxi --profiles-dir dbt/nyc_taxi --select silver_yellow_trips
   ```

3. Silver destination:
   `s3://nyc-taxi-platform-silver/trips/source_month=2025-01/yellow_trips.parquet`

   After approval:
   ```bash
   python scripts/export_silver.py --upload --single-writer
   ```
   Existing objects are logged and skipped before opening DuckDB. Otherwise the
   script validates nonempty output, 25 columns and source lineage, then uses
   DuckDB COPY directly to the final S3 key with Parquet/ZSTD. COPY's returned
   count and a read-back count must match the source count. Permission errors
   abort; only explicit not-found responses mean absence.


## Offline verification

```bash
python -m unittest discover -s tests -p 'test_*.py' -v
python -m compileall -q scripts tests
# Parse/compile without introspecting the S3-backed model:
dbt compile --project-dir dbt/nyc_taxi --profiles-dir dbt/nyc_taxi --no-introspect --no-populate-cache
```

Compilation does not prove AWS connectivity or successful S3 execution.
Offline tests use temporary local files and mocked S3 clients/DuckDB calls.
They never execute S3 COPY.

## Gold MVP (Phase 3)

Gold reads published Silver, not the Bronze-backed `silver_yellow_trips` view.
The `published_silver` input view defaults to:
`s3://nyc-taxi-platform-silver/trips/source_month=2025-01/yellow_trips.parquet`.
Set dbt variable `silver_parquet_path` to an explicit local Parquet for offline work.
The source must retain the 25-column Silver schema, decimal monetary types and
January 2025 file lineage. Gold does not rebuild or change Bronze/Silver.

| Model | Grain | Columns / KPIs |
| --- | --- | --- |
| `fct_trips` | Every retained Silver row | Vendor, pickup/dropoff timestamps and IDs, pickup date, passengers, payment type, distance, duration, trip amount |
| `agg_daily_trips` | Pickup date | total_trips, total_revenue, average_trip_amount, average_trip_distance, average_trip_duration |
| `agg_pickup_location` | Pickup location ID, including one NULL group | total_trips, total_revenue, average_trip_amount |

There is no invented trip ID, deduplication, location name or Taxi Zone lookup.
`trip_amount = COALESCE(total_amount, 0)` in `DECIMAL(18,2)`; revenue sums that
amount, including negative amounts/refunds. Do not sum all monetary columns:
`total_amount` already represents the reported trip total. It is reported trip
revenue, not accounting net revenue. `average_trip_amount` includes zero for
original null amounts and divides by all trips. Counts always use `COUNT(*)`.
Distance (miles) and duration (minutes) averages ignore null values, and return
null if the group has no non-null values. Silver already rejects missing timestamps
and negative durations; zero durations remain valid. NULL pickup location IDs
are preserved as one unknown group and are intentionally exempt from not-null tests.

January 2025 describes the **source file**, not a pickup-date filter. Retain dates
outside January to preserve Silver's row count. The local file contains 33 pickup
dates from 2024-12-31 through 2025-02-01; Gold preserves them.

### Local commands (no AWS writes)

Use an isolated warehouse so existing Silver views remain untouched:

```bash
source .venv/bin/activate
export AWS_PROFILE=nyc-taxi
export NYC_TAXI_DB_PATH="$PWD/data/warehouse/nyc_taxi_gold.duckdb"
# Compile all models without querying S3:
dbt compile --project-dir dbt/nyc_taxi --profiles-dir dbt/nyc_taxi --no-introspect --no-populate-cache
```

If a local published Silver Parquet is available, pass its absolute path below.
Otherwise, produce an offline equivalent using the existing, compiled Silver SQL
and the unchanged local January Bronze input (run the compile command first):

```bash
python - <<'PY'
from pathlib import Path
import duckdb
sql = Path('dbt/nyc_taxi/target/compiled/nyc_taxi/models/silver/silver_yellow_trips.sql').read_text()
sql = sql.replace(
    's3://nyc-taxi-platform-bronze/yellow/year=2025/month=01/yellow_tripdata_2025-01.parquet',
    str(Path('data/raw/yellow_tripdata_2025-01.parquet').resolve()))
output = Path('data/warehouse/silver_local.parquet').resolve()
if output.exists():
    raise FileExistsError(output)
with duckdb.connect() as con:
    con.execute('CREATE TABLE local_silver AS ' + sql)
    con.execute('COPY local_silver TO ? (FORMAT PARQUET, COMPRESSION ZSTD)', [str(output)])
PY

dbt build --project-dir dbt/nyc_taxi --profiles-dir dbt/nyc_taxi \
  --select path:models/gold \
  --vars "{silver_parquet_path: '$PWD/data/warehouse/silver_local.parquet'}"
python -m compileall -q scripts tests
python -m unittest discover -s tests -p 'test_*.py' -v
# Optional if pytest is installed; same unittest-compatible tests:
python -m pytest tests -q
```

The dbt tests verify source
lineage/width, required keys, unique aggregate grains including NULL location,
fact/Silver counts, aggregate/fact counts and exact decimal revenue reconciliation.
The Python edge cases include duplicate trips, null money/duration/distance and refunds.

To read the published S3 source instead, omit `--vars` from the Gold build command.
This reads S3 only, using `AWS_PROFILE=nyc-taxi`; httpfs/aws extensions and working
network/credentials are required. Select `path:models/gold` to avoid running Silver
against Bronze unintentionally.

### Gold destination and approved direct export

No Gold bucket is assumed to exist. The operator must confirm an existing bucket
and verify the profile's `s3:ListBucket` and `s3:PutObject` permissions on the exact
prefixes before an approved upload. A read-only `head-bucket`/`head-object` check
cannot prove PutObject permission; verify the applicable IAM/bucket policies with
the AWS owner. Do not create a bucket or modify IAM for this script.

Predictable keys, where `<gold-bucket>` is an explicitly verified bucket:

```text
s3://<gold-bucket>/fct_trips/source_month=2025-01/fct_trips.parquet
s3://<gold-bucket>/agg_daily_trips/source_month=2025-01/agg_daily_trips.parquet
s3://<gold-bucket>/agg_pickup_location/source_month=2025-01/agg_pickup_location.parquet
```

Only after permission verification and explicit AWS write approval:

```bash
AWS_PROFILE=nyc-taxi aws s3api head-bucket --bucket <gold-bucket>
python scripts/export_gold.py --db-path "$NYC_TAXI_DB_PATH" \
  --upload --single-writer --bucket <gold-bucket>
```

Both exporters use `AWS_PROFILE` (set to `nyc-taxi` above) for boto3 and
DuckDB's config credential provider. Gold checks bucket access and all destination
objects before opening DuckDB, then validates every pending model's schema and
nonempty row count before the first COPY. Existing objects are logged and skipped;
permission errors abort. Each COPY goes directly to its existing final key and
checks COPY's returned row count plus the S3 Parquet read-back count.
There is no local export directory, boto3 upload, staging, publish, or cleanup step.

### Direct S3 write limitations

`--upload` requires prior owner approval. `--single-writer` acknowledges that the
operator has ensured exclusive access to these destination keys for the entire
export, including other processes and external writers. It does not acquire a lock.
Do not use these commands when exclusive access cannot be ensured. The HEAD check
and COPY are separate operations: direct COPY does not retain the old conditional
`IfNoneMatch="*"` upload protection. A concurrent writer can create an object after
HEAD and have it overwritten. No overwrite option or automatic overwrite retry is
provided; existing objects found by HEAD are always skipped.

DuckDB uses multipart S3 uploads, so the old single-PutObject 5 GB check is removed.
See [DuckDB S3 support](https://duckdb.org/docs/current/core_extensions/httpfs/s3api).
The httpfs/aws extensions, network, valid credentials and S3 read/write/multipart
permissions are required. Read-back validation adds an S3 read and happens after
the final object has been written; failure cannot roll it back. A failed export
may leave an object, a partial Gold set, or unfinished multipart uploads. Inspect
failed runs before rerunning: existing keys are skipped without revalidation.
There is no automatic deletion or repair. Outputs across Gold models are not atomic.

Run dbt build/tests successfully before export and do not rebuild the warehouse
concurrently. Publishing revised results at existing keys requires a separate
approved operational decision. There is no orchestration or incremental load.
Offline verification does not prove AWS permissions, connectivity, multipart
behavior, or successful S3 execution. No AWS writes were performed during this
refactor.
