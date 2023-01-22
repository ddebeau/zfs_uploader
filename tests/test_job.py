from unittest import TestCase
import subprocess
import warnings

from zfs_uploader.config import Config
from zfs_uploader.zfs import (create_filesystem, destroy_filesystem,
                              destroy_snapshot, load_key, mount_filesystem,
                              SUBPROCESS_KWARGS)


class JobTestsBase:
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

    def test_start_multiple_full(self):
        """ Test job start with multiple full backups. """
        # Given
        self.job._max_incremental_backups_per_full = 1

        # When
        for _ in range(4):
            self.job.start()

        # Then
        backups_full = self.job._backup_db.get_backups(backup_type='full')
        self.assertEqual(2, len(backups_full))

        backups_inc = self.job._backup_db.get_backups(backup_type='inc')
        self.assertEqual(2, len(backups_inc))

    def test_start_only_full(self):
        """ Test job start with only full backups. """
        # Given
        self.job._max_incremental_backups_per_full = 0

        # When
        for _ in range(3):
            self.job.start()

        # Then
        backups_full = self.job._backup_db.get_backups(backup_type='full')
        self.assertEqual(3, len(backups_full))

        backups_inc = self.job._backup_db.get_backups(backup_type='inc')
        self.assertEqual(0, len(backups_inc))

    def test_restore_from_full_backup(self):
        """ Test restore from full backup. """
        # Given
        self.job.start()

        out = destroy_filesystem(self.job.filesystem)
        self.assertEqual(0, out.returncode, msg=out.stderr)

        # When
        self.job.restore()
        if self.encrypted_test:
            out = load_key(self.job.filesystem, 'file:///test_key')
            self.assertEqual(0, out.returncode, msg=out.stderr)

            out = mount_filesystem(self.job.filesystem)
            self.assertEqual(0, out.returncode, msg=out.stderr)

        # Then
        with open(self.test_file, 'r') as f:
            out = f.read()
        self.assertEqual(self.test_data, out)

    def test_restore_from_full_backup_with_new_data(self):
        """ Test restore from full backup with new data. """
        # Given
        self.job.start()

        snapshot_names = self.job._snapshot_db.get_snapshot_names()
        out = destroy_snapshot(self.job.filesystem, snapshot_names[-1])
        self.assertEqual(0, out.returncode, msg=out.stderr)

        with open(self.test_file, 'w') as f:
            f.write('new data')

        # When
        self.job.restore()
        if self.encrypted_test:
            out = load_key(self.job.filesystem, 'file:///test_key')
            self.assertEqual(0, out.returncode, msg=out.stderr)

            out = mount_filesystem(self.job.filesystem)
            self.assertEqual(0, out.returncode, msg=out.stderr)

        # Then
        with open(self.test_file, 'r') as f:
            out = f.read()
        self.assertEqual(self.test_data, out)

    def test_restore_from_incremental_backup(self):
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
        if self.encrypted_test:
            out = load_key(self.job.filesystem, 'file:///test_key')
            self.assertEqual(0, out.returncode, msg=out.stderr)

            out = mount_filesystem(self.job.filesystem)
            self.assertEqual(0, out.returncode, msg=out.stderr)

        # Then
        with open(self.test_file, 'r') as f:
            out = f.read()
        self.assertEqual(self.test_data + 'append', out)

    def test_restore_from_incremental_backup_with_new_data(self):
        """ Test restore from incremental backup with new data. """
        # Given
        self.job.start()

        with open(self.test_file, 'a') as f:
            f.write('append')
        self.job.start()

        snapshot_names = self.job._snapshot_db.get_snapshot_names()
        out = destroy_snapshot(self.job.filesystem, snapshot_names[-1])
        self.assertEqual(0, out.returncode, msg=out.stderr)

        with open(self.test_file, 'w') as f:
            f.write('new data')

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
        if self.encrypted_test:
            out = load_key(self.job.filesystem, 'file:///test_key')
            self.assertEqual(0, out.returncode, msg=out.stderr)

            out = mount_filesystem(self.job.filesystem)
            self.assertEqual(0, out.returncode, msg=out.stderr)

        # Then
        with open(self.test_file, 'r') as f:
            out = f.read()
        self.assertEqual(self.test_data + 'append', out)

    def test_restore_from_full_backup_after_incremental_backup(self):
        """ Test restoring from a full backup after an incremental backup has
        been taken without destroying the file system. """
        # Given
        self.job.start()

        with open(self.test_file, 'a') as f:
            f.write('append')
        self.job.start()

        snapshot_names = self.job._snapshot_db.get_snapshot_names()
        out = destroy_snapshot(self.job.filesystem, snapshot_names[0])
        self.assertEqual(0, out.returncode, msg=out.stderr)

        # When
        backups = self.job._backup_db.get_backup_times('full')
        self.job.restore(backups[0])
        if self.encrypted_test:
            out = load_key(self.job.filesystem, 'file:///test_key')
            self.assertEqual(0, out.returncode, msg=out.stderr)

            out = mount_filesystem(self.job.filesystem)
            self.assertEqual(0, out.returncode, msg=out.stderr)

        # Then
        with open(self.test_file, 'r') as f:
            out = f.read()
        self.assertEqual(self.test_data, out)

    def test_restore_from_incremental_backup_after_incremental_backup(self):
        """ Test restoring from an incremental backup after an incremental
        backup has been taken without destroying the file system. """
        # Given
        self.job.start()

        with open(self.test_file, 'a') as f:
            f.write('append')
        self.job.start()

        with open(self.test_file, 'a') as f:
            f.write('append again')
        self.job.start()

        snapshot_names = self.job._snapshot_db.get_snapshot_names()
        out = destroy_snapshot(self.job.filesystem, snapshot_names[1])
        self.assertEqual(0, out.returncode, msg=out.stderr)

        # When
        backups = self.job._backup_db.get_backup_times('inc')
        self.job.restore(backups[0])

        # Then
        with open(self.test_file, 'r') as f:
            out = f.read()
        self.assertEqual(self.test_data + 'append', out)

    def test_restore_to_different_filesystem(self):
        """ Test restore to a different filesystem. """
        # Given
        self.job.start()

        # When
        self.job.restore(filesystem=self.filesystem_2)
        if self.encrypted_test:
            out = load_key(self.filesystem_2, 'file:///test_key')
            self.assertEqual(0, out.returncode, msg=out.stderr)

            out = mount_filesystem(self.filesystem_2)
            self.assertEqual(0, out.returncode, msg=out.stderr)

        # Then
        test_file = f'/{self.filesystem_2}/test_file'
        with open(test_file, 'r') as f:
            out = f.read()
        self.assertEqual(self.test_data, out)

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
        """ Test the backup limiter. """

        # Given
        self.job._max_incremental_backups_per_full = 1

        for _ in range(4):
            self.job.start()

        backups_full = self.job._backup_db.get_backups(backup_type='full')
        self.assertEqual(2, len(backups_full))

        backups_inc = self.job._backup_db.get_backups(backup_type='inc')
        self.assertEqual(2, len(backups_inc))

        # When
        self.job._max_backups = 3
        self.job._limit_backups()

        # Then
        # Only the oldest incremental backup should be removed.
        backups_full_new = self.job._backup_db.get_backups(backup_type='full')
        self.assertEqual(backups_full, backups_full_new)

        backups_inc_new = self.job._backup_db.get_backups(backup_type='inc')
        self.assertEqual(backups_inc[1:], backups_inc_new)

        # When
        self.job._max_backups = 2
        self.job._limit_backups()

        # Then
        # The oldest full backup should be removed.
        backups_full_new = self.job._backup_db.get_backups(backup_type='full')
        self.assertEqual(backups_full[1:], backups_full_new)

        backups_inc_new = self.job._backup_db.get_backups(backup_type='inc')
        self.assertEqual(backups_inc[1:], backups_inc_new)

    def test_limit_backups_one_full(self):
        """ Test the backup limiter when there's only one full backup. """

        # Given
        for _ in range(3):
            self.job.start()

        backups_full = self.job._backup_db.get_backups(backup_type='full')
        self.assertEqual(1, len(backups_full))

        backups_inc = self.job._backup_db.get_backups(backup_type='inc')
        self.assertEqual(2, len(backups_inc))

        # When
        self.job._max_backups = 2
        self.job._limit_backups()

        # Then
        # Only the oldest incremental backup should be removed.
        backups_full_new = self.job._backup_db.get_backups(backup_type='full')
        self.assertEqual(backups_full, backups_full_new)

        backups_inc_new = self.job._backup_db.get_backups(backup_type='inc')
        self.assertEqual(backups_inc[1:], backups_inc_new)

        # When
        self.job._max_backups = 1
        self.job._limit_backups()

        # Then
        # Only the full backup should remain.
        backups_full_new = self.job._backup_db.get_backups(backup_type='full')
        self.assertEqual(backups_full, backups_full_new)

        backups_inc_new = self.job._backup_db.get_backups(backup_type='inc')
        self.assertEqual(0, len(backups_inc_new))

    def test_limit_backups_all_full(self):
        """ Test the backup limiter when it's only full backups. """

        # Given
        self.job._max_incremental_backups_per_full = 0
        for _ in range(3):
            self.job.start()

        backups_full = self.job._backup_db.get_backups(backup_type='full')
        self.assertEqual(3, len(backups_full))

        backups_inc = self.job._backup_db.get_backups(backup_type='inc')
        self.assertEqual(0, len(backups_inc))

        # When
        self.job._max_backups = 2
        self.job._limit_backups()

        # Then
        # Only the oldest full backup should be removed.
        backups_full_new = self.job._backup_db.get_backups(backup_type='full')
        self.assertEqual(backups_full[1:], backups_full_new)


class JobTestsUnencrypted(JobTestsBase, TestCase):
    def setUp(self):
        self.encrypted_test = False
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

        self.filesystem_2 = 'test-pool/test-filesystem-2'

    def tearDown(self):
        for filesystem in [self.job.filesystem, self.filesystem_2]:
            out = destroy_filesystem(filesystem)
            if out.returncode:
                self.assertIn('dataset does not exist', out.stderr)

        for item in self.bucket.objects.all():
            item.delete()


class JobTestsEncrypted(JobTestsBase, TestCase):
    def setUp(self):
        self.encrypted_test = True
        warnings.filterwarnings("ignore", category=ResourceWarning,
                                message="unclosed.*<ssl.SSLSocket.*>")

        config = Config('config.cfg')
        self.job = next(iter(config.jobs.values()))
        self.bucket = self.job.bucket
        self.test_file = f'/{self.job.filesystem}/test_file'
        self.test_data = str(list(range(100_000)))

        with open('/test_key', 'w') as f:
            f.write('test_key')
        out = subprocess.run(
            ['zfs', 'create', '-o', 'encryption=on', '-o',
             'keyformat=passphrase', '-o', 'keylocation=file:///test_key',
             self.job.filesystem],
            **SUBPROCESS_KWARGS
        )
        self.assertEqual(0, out.returncode, msg=out.stderr)

        with open(self.test_file, 'w') as f:
            f.write(self.test_data)

        self.filesystem_2 = 'test-pool/test-filesystem-2'

    def tearDown(self):
        for filesystem in [self.job.filesystem, self.filesystem_2]:
            out = destroy_filesystem(filesystem)
            if out.returncode:
                self.assertIn('dataset does not exist', out.stderr)

        for item in self.bucket.objects.all():
            item.delete()
