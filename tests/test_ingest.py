import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock
from botocore.exceptions import ClientError
from scripts import ingest


class IngestTests(unittest.TestCase):
    def test_key(self):
        self.assertEqual(ingest.build_object_key('yellow', 2025, 1), 'yellow/year=2025/month=01/yellow_tripdata_2025-01.parquet')

    def test_checksum(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'input'
            path.write_bytes(b'taxi')
            self.assertEqual(ingest.calculate_sha256(path), hashlib.sha256(b'taxi').hexdigest())

    def test_existing_checksum_decisions(self):
        for metadata, expected in (({'sha256': 'same'}, 'SKIP'), ({'sha256': 'other'}, 'FAIL'), ({}, 'FAIL')):
            s3 = Mock()
            s3.head_object.return_value = {'Metadata': metadata}
            self.assertEqual(ingest.check_upload_status(s3, 'bucket', 'key', 'same'), expected)

    def test_missing_and_denied(self):
        s3 = Mock()
        s3.head_object.side_effect = ClientError({'Error': {'Code': '404'}}, 'HeadObject')
        self.assertEqual(ingest.check_upload_status(s3, 'bucket', 'key', 'same'), 'UPLOAD')
        s3.head_object.side_effect = ClientError({'Error': {'Code': '403'}}, 'HeadObject')
        with self.assertRaises(ClientError):
            ingest.check_upload_status(s3, 'bucket', 'key', 'same')
