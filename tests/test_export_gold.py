import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import duckdb
from botocore.exceptions import ClientError

from scripts import export_gold as gold

ROOT = Path(__file__).resolve().parents[1]


def error(code):
    return ClientError({'Error': {'Code': code}}, 'S3')


class GoldTests(unittest.TestCase):
    def setUp(self):
        self.con = duckdb.connect()
        self.addCleanup(self.con.close)
        self.con.execute('''create table published_silver as select
            1::integer VendorID, timestamp '2025-01-01 00:00:00' tpep_pickup_datetime,
            timestamp '2025-01-01 00:05:00' tpep_dropoff_datetime,
            date '2025-01-01' pickup_date, null::integer PULocationID,
            2::integer DOLocationID, 1::bigint passenger_count, 1::bigint payment_type,
            2.0::double trip_distance, 5.0::double trip_duration_minutes,
            10::decimal(18,2) total_amount''')
        # Identical trips must survive; include null amounts/durations and a refund.
        self.con.execute('insert into published_silver select * from published_silver')
        self.con.execute('''insert into published_silver select VendorID,
            tpep_pickup_datetime, tpep_dropoff_datetime, pickup_date, 3,
            DOLocationID, passenger_count, payment_type, null, null, null
            from published_silver limit 1''')
        self.con.execute('''insert into published_silver select VendorID,
            tpep_pickup_datetime, tpep_dropoff_datetime, pickup_date, 3,
            DOLocationID, passenger_count, payment_type, trip_distance,
            trip_duration_minutes, -2 from published_silver limit 1''')
        for model in gold.MODELS:
            sql = (ROOT / f'dbt/nyc_taxi/models/gold/{model}.sql').read_text()
            sql = sql.split('}}', 1)[1]
            for source in ['published_silver', 'fct_trips']:
                sql = sql.replace("{{ ref('" + source + "') }}", source)
            self.con.execute(f'create table {model} as {sql}')

    def test_grains_null_policy_and_reconciliation(self):
        self.assertEqual(self.con.execute('select count(*), sum(trip_amount) from fct_trips').fetchone(), (4, 18))
        self.assertEqual(self.con.execute('select total_trips, total_revenue, average_trip_amount, average_trip_duration, average_trip_distance from agg_daily_trips').fetchone(), (4, 18, 4.5, 5.0, 2.0))
        self.assertEqual(self.con.execute('select total_trips, total_revenue from agg_pickup_location where pickup_location_id is null').fetchone(), (2, 20))
        self.assertEqual(self.con.execute('select sum(total_trips), sum(total_revenue) from agg_pickup_location').fetchone(), (4, 18))

    def test_round_trip(self):
        with tempfile.TemporaryDirectory() as d:
            for model in gold.MODELS:
                rows = gold.check_model(self.con, model)
                gold.export_model(self.con, model, str(Path(d) / f'{model}.parquet'), rows)

    def test_wrong_schema_fails(self):
        self.con.execute('alter table fct_trips drop vendor_id')
        with self.assertRaises(ValueError):
            gold.check_model(self.con, 'fct_trips')

    def test_empty_source_fails(self):
        self.con.execute('delete from fct_trips')
        with self.assertRaises(ValueError):
            gold.check_model(self.con, 'fct_trips')

    def test_direct_s3_copy(self):
        con = Mock()
        con.execute.return_value.fetchone.side_effect = [(4,), (4,)]
        destination = 's3://gold-bucket/' + gold.key_for('fct_trips')
        gold.export_model(con, 'fct_trips', destination, 4)
        self.assertEqual(con.execute.call_args_list[0].args,
                         ('COPY fct_trips TO ? (FORMAT PARQUET, COMPRESSION ZSTD)', [destination]))

    def test_count_mismatch(self):
        con = Mock()
        con.execute.return_value.fetchone.side_effect = [(4,), (3,)]
        with self.assertRaisesRegex(ValueError, 'row count mismatch'):
            gold.export_model(con, 'fct_trips', 's3://bucket/file.parquet', 4)

    def test_existing_objects_skip_without_db(self):
        with patch.object(gold.boto3, 'client'), patch.object(gold, 'silver_connection') as connect:
            gold.main(['--upload', '--single-writer', '--bucket', 'gold-bucket'])
            connect.assert_not_called()

    def test_bucket_and_head_errors_prevent_writes(self):
        for operation in ['head_bucket', 'head_object']:
            with self.subTest(operation=operation), patch.object(gold.boto3, 'client') as client, patch.object(gold, 'silver_connection') as connect:
                getattr(client.return_value, operation).side_effect = error('AccessDenied')
                with self.assertRaises((ClientError, RuntimeError)):
                    gold.main(['--upload', '--single-writer', '--bucket', 'gold-bucket'])
                connect.assert_not_called()

    def test_requires_write_opt_in(self):
        with patch.object(gold.boto3, 'client') as client:
            with self.assertRaises(SystemExit):
                gold.main(['--bucket', 'gold-bucket'])
            client.assert_not_called()

    def test_all_models_validated_before_copy(self):
        with patch.object(gold.boto3, 'client') as client, patch.object(gold, 'silver_connection') as connect, patch.object(gold, 'export_model') as export:
            client.return_value.head_object.side_effect = error('404')
            connect.return_value.__enter__.return_value = self.con
            self.con.execute('delete from agg_daily_trips')
            with self.assertRaises(RuntimeError):
                gold.main(['--upload', '--single-writer', '--bucket', 'gold-bucket'])
            export.assert_not_called()
