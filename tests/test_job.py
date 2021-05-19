import unittest
import warnings

from zfs_uploader.config import Config
from zfs_uploader.zfs import create_filesystem, destroy_filesystem


class JobTests(unittest.TestCase):
    def setUp(self):
        warnings.filterwarnings("ignore", category=ResourceWarning,
                                message="unclosed.*<ssl.SSLSocket.*>")

        config = Config('config.cfg')
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
        backups = self.job._backup_db.get_backups(backup_type='full')
        self.assertEqual(1, len(backups))
        self.assertEqual('full', backups[0].backup_type)

    def test_start_incremental(self):
        """ Test job start with incremental backup. """
        # Given
        self.job.start()

        backup_type = 'full'
        backups = self.job._backup_db.get_backups(backup_type)
        self.assertEqual(1, len(backups))
        self.assertEqual(backup_type, backups[0].backup_type)

        # When
        self.job.start()

        # Then
        backup_type = 'inc'
        backups = self.job._backup_db.get_backups(backup_type)
        self.assertEqual(1, len(backups))
        self.assertEqual(backup_type, backups[0].backup_type)

    def test_restore_from_full_backup(self):
        """ Test restore from full backup. """
        # Given
        self.job.start()

        out = destroy_filesystem(self.job.filesystem)
        self.assertEqual(0, out.returncode, msg=out.stderr)

        # When
        self.job.restore()

        # Then
        with open(self.test_file, 'r') as f:
            out = f.read()
        self.assertEqual(self.test_data, out)

    def test_restore_from_increment_backup(self):
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

    def test_restore_specific_backup(self):
        """ Test restoring from specific backup. """
        # Given
        self.job.start()

        with open(self.test_file, 'a') as f:
            f.write('append')
        self.job.start()

        out = destroy_filesystem(self.job.filesystem)
        self.assertEqual(0, out.returncode, msg=out.stderr)

        # When
        backups = self.job._backup_db.get_backup_times('inc')
        self.job.restore(backups[0])

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

        snapshots = self.job._snapshot_db.get_snapshots()
        self.assertEqual(4, len(snapshots))

        # When
        self.job._max_snapshots = 2
        self.job._limit_snapshots()

        # Then
        snapshots_new = self.job._snapshot_db.get_snapshots()
        self.assertEqual(3, len(snapshots_new))
        # Check if two most recent snapshots exist.
        self.assertListEqual(snapshots[-2:], snapshots_new[-2:])
        # Check if full backup snapshot exists.
        self.assertEqual(snapshots[0], snapshots_new[0])

    def test_limit_backups(self):
        """ Test the incremental backup limiter. """

        # Given
        self.job._max_snapshots = None
        self.job._max_incremental_backups = 4

        for _ in range(5):
            self.job.start()

        backups_full = self.job._backup_db.get_backups(backup_type='full')
        self.assertEqual(1, len(backups_full))

        backups_inc = self.job._backup_db.get_backups(backup_type='inc')
        self.assertEqual(4, len(backups_inc))

        # When
        self.job._max_incremental_backups = 2
        self.job._limit_incremental_backups()

        # Then
        backups_inc_new = self.job._backup_db.get_backups(
            backup_type='inc')
        self.assertEqual(2, len(backups_inc_new))

        # The two most recent incremental backups should exist.
        self.assertEqual(backups_inc[-2:], backups_inc_new)
