import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import duckdb
from botocore.exceptions import ClientError

from scripts import export_silver as pipeline


def aws_error(code):
    return ClientError({'Error': {'Code': code}}, 'S3')


class ExportSilverTests(unittest.TestCase):
    def setUp(self):
        self.con = duckdb.connect(':memory:')
        self.addCleanup(self.con.close)
        columns = ', '.join(f'{i}::INTEGER AS value_{i}' for i in range(23))
        self.con.execute(f"""CREATE TABLE silver_yellow_trips AS SELECT {columns},
            '2025-01'::VARCHAR AS source_month,
            'yellow_tripdata_2025-01.parquet'::VARCHAR AS source_file""")

    def test_local_export(self):
        rows, schema = pipeline.check_silver_view(self.con, '2025-01')
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / 'silver.parquet')
            pipeline.export_parquet(self.con, path, rows)
            self.assertEqual(self.con.execute('SELECT COUNT(*) FROM read_parquet(?)', [path]).fetchone()[0], rows)
            self.assertEqual([(r[0], r[1]) for r in self.con.execute('DESCRIBE SELECT * FROM read_parquet(?)', [path]).fetchall()], schema)

    def test_wrong_lineage_rejected(self):
        for assignment in ("source_month = '2025-02'", "source_file = NULL"):
            with self.subTest(assignment=assignment):
                self.con.execute('BEGIN')
                self.con.execute('UPDATE silver_yellow_trips SET ' + assignment)
                with self.assertRaisesRegex(ValueError, 'invalid lineage'):
                    pipeline.check_silver_view(self.con, '2025-01')
                self.con.execute('ROLLBACK')

    def test_empty_source_rejected(self):
        self.con.execute('DELETE FROM silver_yellow_trips')
        with self.assertRaisesRegex(ValueError, 'empty'):
            pipeline.check_silver_view(self.con, '2025-01')

    def test_wrong_column_count_rejected(self):
        self.con.execute('ALTER TABLE silver_yellow_trips DROP value_0')
        with self.assertRaisesRegex(ValueError, 'column count'):
            pipeline.check_silver_view(self.con, '2025-01')

    def test_existing_final_skips_before_connection(self):
        with patch.object(pipeline.boto3, 'client'), patch.object(pipeline, 'silver_connection') as connect:
            pipeline.main(['--upload', '--single-writer'])
            connect.assert_not_called()

    def test_permission_error_prevents_copy(self):
        with patch.object(pipeline.boto3, 'client') as client, patch.object(pipeline, 'silver_connection') as connect:
            client.return_value.head_object.side_effect = aws_error('403')
            with self.assertRaisesRegex(RuntimeError, 'AWS 403'):
                pipeline.main(['--upload', '--single-writer'])
            connect.assert_not_called()

    def test_direct_s3_copy_and_validation(self):
        con = Mock()
        con.execute.return_value.fetchone.side_effect = [(7,), (7,)]
        destination = 's3://bucket/trips.parquet'
        pipeline.export_parquet(con, destination, 7)
        self.assertIn('COPY silver_yellow_trips TO ?', con.execute.call_args_list[0].args[0])
        self.assertEqual(con.execute.call_args_list[0].args[1], [destination])

    def test_count_mismatch(self):
        con = Mock()
        con.execute.return_value.fetchone.side_effect = [(7,), (6,)]
        with self.assertRaisesRegex(ValueError, 'row count mismatch'):
            pipeline.export_parquet(con, 's3://bucket/trips.parquet', 7)

    def test_requires_write_opt_in(self):
        with patch.object(pipeline.boto3, 'client') as client:
            with self.assertRaises(SystemExit):
                pipeline.main([])
            client.assert_not_called()

    def test_copy_failure_has_context(self):
        with patch.object(pipeline.boto3, 'client') as client, patch.object(pipeline, 'silver_connection') as connect:
            client.return_value.head_object.side_effect = aws_error('404')
            connect.return_value.__enter__.return_value = self.con
            with patch.object(pipeline, 'export_parquet', side_effect=duckdb.IOException('failed')):
                with self.assertRaisesRegex(RuntimeError, 'Output may exist'):
                    pipeline.main(['--upload', '--single-writer'])
