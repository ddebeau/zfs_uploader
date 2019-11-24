from time import sleep
import unittest
import warnings

from zfs_uploader.config import Config
from zfs_uploader.zfs import create_filesystem, destroy_filesystem


class JobTests(unittest.TestCase):
    def setUp(self):
        warnings.filterwarnings("ignore", category=ResourceWarning,
                                message="unclosed.*<ssl.SSLSocket.*>")

        config = Config('../config.cfg')
        self.job = next(iter(config.jobs.values()))
        self.bucket = self.job.bucket
        self.test_file = f'/{self.job.filesystem}/test_file'
        self.test_data = str(list(range(100_000)))

        out = create_filesystem(self.job.filesystem)
        self.assertEqual(0, out.returncode, msg=out.stderr)

        with open(self.test_file, 'w') as f:
            f.write(self.test_data)

    def tearDown(self):
        out = destroy_filesystem(self.job.filesystem)
        if out.returncode:
            self.assertIn('dataset does not exist', out.stderr)

        for item in self.bucket.objects.all():
            item.delete()

    def test_start_full(self):
        # When
        self.job.start()

        # Then
        for item in self.bucket.objects.all():
            self.assertIn('full', item.key)

    def test_start_incremental(self):
        # Given
        self.job.start()

        for item in self.bucket.objects.all():
            self.assertIn('full', item.key)

        # wait until snapshot name changes
        sleep(1)

        # When
        self.job.start()

        backup_inc = []
        for item in self.bucket.objects.all():
            if 'inc' in item.key:
                backup_inc.append(item.key)

        self.assertEqual(len(backup_inc), 1)
