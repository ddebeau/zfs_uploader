import unittest
import warnings

from config import Config
from upload import get_s3_client
from zfs import (create_filesystem, create_snapshot, destroy_filesystem,
                 open_snapshot_stream)


class IntegrationTests(unittest.TestCase):
    def setUp(self):
        # Given
        warnings.filterwarnings("ignore", category=ResourceWarning,
                                message="unclosed.*<ssl.SSLSocket.*>")

        self.config = Config()
        self.s3 = get_s3_client(self.config)
        self.bucket = self.config.bucket
        self.filesystem = 'hgstPool/test'
        self.snapshot_name = 'snap'
        self.test_file = f'/{self.filesystem}/test_file'

        out = create_filesystem(self.filesystem)
        self.assertEqual(0, out.returncode, msg=out.stderr)

        with open(self.test_file, 'w') as f:
            f.write('hello world')

    def tearDown(self):
        out = destroy_filesystem(self.filesystem)
        self.assertEqual(0, out.returncode, msg=out.stderr)

        self.s3.delete_object(Bucket=self.bucket, Key=self.snapshot_name)

    def test_restore_filesystem(self):
        """ Restore filesystem from S3. """
        # Given
        out = create_snapshot(self.filesystem, self.snapshot_name)
        self.assertEqual(0, out.returncode, msg=out.stderr)

        # When
        with open_snapshot_stream(self.filesystem, self.snapshot_name,
                                  'r') as f:
            self.s3.upload_fileobj(f.stdout, self.bucket, self.snapshot_name)
            stderr = f.stderr.read().decode('utf-8')
        self.assertEqual(0, f.returncode, msg=stderr)

        out = destroy_filesystem(self.filesystem)
        self.assertEqual(0, out.returncode, msg=out.stderr)

        # Then
        with open_snapshot_stream(self.filesystem, self.snapshot_name,
                                  'w') as f:
            self.s3.download_fileobj(self.bucket, self.snapshot_name, f.stdin)
            stderr = f.stderr.read().decode('utf-8')
        self.assertEqual(0, f.returncode, msg=stderr)

        with open(self.test_file, 'r') as f:
            out = f.read()

        self.assertEqual('hello world', out)
