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
        """ Test job start with full backup. """
        # When
        self.job.start()

        # Then
        backup_info = self.job._get_backup_info()
        self.assertEqual('full', backup_info[-1]['backup_type'])

    def test_start_incremental(self):
        """ Test job start with incremental backup. """
        # Given
        self.job.start()

        backup_info = self.job._get_backup_info()
        self.assertEqual('full', backup_info[-1]['backup_type'])

        # wait until snapshot name changes
        sleep(1)

        # When
        self.job.start()

        # Then
        backup_info = self.job._get_backup_info()
        self.assertEqual('inc', backup_info[-1]['backup_type'])

    def test_restore_from_increment(self):
        """ Test restore from incremental backup. """
        # Given
        self.job.start()

        with open(self.test_file, 'a') as f:
            f.write('append')

        # wait until snapshot name changes
        sleep(1)
        self.job.start()

        out = destroy_filesystem(self.job.filesystem)
        self.assertEqual(0, out.returncode, msg=out.stderr)

        # When
        self.job.restore()

        # Then
        with open(self.test_file, 'r') as f:
            out = f.read()
        self.assertEqual(self.test_data + 'append', out)
