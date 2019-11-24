import unittest
import warnings

from zfs_uploader.config import Config
from zfs_uploader.zfs import (create_filesystem, create_snapshot,
                              destroy_filesystem, open_snapshot_stream)


class IntegrationTests(unittest.TestCase):
    def setUp(self):
        # Given
        warnings.filterwarnings("ignore", category=ResourceWarning,
                                message="unclosed.*<ssl.SSLSocket.*>")

        config = Config('../config.cfg')
        self.job = next(iter(config.jobs.values()))
        self.bucket = self.job.bucket
        self.filesystem = self.job.filesystem
        self.snapshot_name = 'snap'
        self.test_file = f'/{self.filesystem}/test_file'

        out = create_filesystem(self.filesystem)
        self.assertEqual(0, out.returncode, msg=out.stderr)

        with open(self.test_file, 'w') as f:
            f.write('hello world')

    def tearDown(self):
        out = destroy_filesystem(self.filesystem)
        self.assertEqual(0, out.returncode, msg=out.stderr)

        self.bucket.delete_objects(
            Delete={'Objects': [{'Key': self.snapshot_name}]})

    def test_restore_filesystem(self):
        """ Restore filesystem from S3. """
        # Given
        out = create_snapshot(self.filesystem, self.snapshot_name)
        self.assertEqual(0, out.returncode, msg=out.stderr)

        # When
        with open_snapshot_stream(self.filesystem, self.snapshot_name,
                                  'r') as f:
            self.bucket.upload_fileobj(f.stdout, self.snapshot_name)
            stderr = f.stderr.read().decode('utf-8')
        self.assertEqual(0, f.returncode, msg=stderr)

        out = destroy_filesystem(self.filesystem)
        self.assertEqual(0, out.returncode, msg=out.stderr)

        # Then
        with open_snapshot_stream(self.filesystem, self.snapshot_name,
                                  'w') as f:
            self.bucket.download_fileobj(self.snapshot_name, f.stdin)
            stderr = f.stderr.read().decode('utf-8')
        self.assertEqual(0, f.returncode, msg=stderr)

        with open(self.test_file, 'r') as f:
            out = f.read()

        self.assertEqual('hello world', out)
