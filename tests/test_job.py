import unittest
import warnings

from zfs_uploader.config import Config
from zfs_uploader.zfs import (create_filesystem, destroy_filesystem,
                              list_snapshots)


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
        backup_info = self.job._read_backup_info()
        backup_keys = list(backup_info.keys())
        self.assertEqual('full', backup_info[backup_keys[-1]]['backup_type'])

    def test_start_incremental(self):
        """ Test job start with incremental backup. """
        # Given
        self.job.start()

        backup_info = self.job._read_backup_info()
        backup_keys = list(backup_info.keys())
        self.assertEqual('full', backup_info[backup_keys[-1]]['backup_type'])

        # When
        self.job.start()

        # Then
        backup_info = self.job._read_backup_info()
        backup_keys = list(backup_info.keys())
        self.assertEqual('inc', backup_info[backup_keys[-1]]['backup_type'])

    def test_restore_from_increment(self):
        """ Test restore from incremental backup. """
        # Given
        self.job.start()

        with open(self.test_file, 'a') as f:
            f.write('append')
        self.job.start()

        out = destroy_filesystem(self.job.filesystem)
        self.assertEqual(0, out.returncode, msg=out.stderr)

        # When
        self.job.restore()

        # Then
        with open(self.test_file, 'r') as f:
            out = f.read()
        self.assertEqual(self.test_data + 'append', out)

    def test_limit_snapshots(self):
        """ Test the snapshot number limiter. """
        # Given
        self.job._max_snapshots = 4

        for _ in range(4):
            self.job.start()

        snapshot_keys = list(list_snapshots().keys())
        self.assertEqual(4, len(snapshot_keys))

        # When
        self.job._max_snapshots = 2
        self.job._limit_snapshots()

        # Then
        out = list(list_snapshots().keys())
        # Check if two most recent snapshots exist.
        self.assertListEqual(snapshot_keys[-2:], out[-2:])
        # Check if full snapshot exists.
        self.assertEqual(snapshot_keys[0], out[0])
        self.assertEqual(3, len(out))

    def test_limit_backups(self):
        """ Test the incremental backup limiter. """

        # Given
        self.job._max_snapshots = None
        self.job._max_incremental_backups = 4

        for _ in range(5):
            self.job.start()

        backup_info = self.job._read_backup_info()
        backup_keys = list(backup_info.keys())
        self.assertEqual(5, len(backup_info))

        # When
        self.job._max_incremental_backups = 2
        self.job._limit_backups()

        # Then
        # The two most recent incremental backups should exist.
        out = self.job._read_backup_info()
        out_keys = list(out.keys())
        self.assertEqual(backup_keys[-2:], out_keys[-2:])
        self.assertEqual(3, len(out))
