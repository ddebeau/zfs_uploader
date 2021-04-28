import unittest
import warnings

from zfs_uploader.config import Config
from zfs_uploader.backup_db import BackupDB


class BackupDBTests(unittest.TestCase):
    def setUp(self):
        warnings.filterwarnings("ignore", category=ResourceWarning,
                                message="unclosed.*<ssl.SSLSocket.*>")

        config = Config('config.cfg')
        self.job = next(iter(config.jobs.values()))
        self.bucket = self.job.bucket
        self.file_system = self.job.filesystem

    def tearDown(self):
        for item in self.bucket.objects.all():
            item.delete()

    def test_create_backup_db(self):
        # Given
        backup_db = BackupDB(self.bucket, self.file_system)
        backup_time = '20210425_201838'
        backup_type = 'full'
        s3_key = f'{self.file_system}/{backup_time}.{backup_type}'

        # When
        backup_db.create_backup(backup_time, backup_type, s3_key)

        # Then
        backup_db_new = BackupDB(self.bucket, self.file_system)

        self.assertEqual(
            backup_db.get_backup(backup_time),
            backup_db_new.get_backup(backup_time)
        )

    def test_delete_backup_db(self):
        # Given
        backup_db = BackupDB(self.bucket, self.file_system)
        backup_time = '20210425_201838'
        backup_type = 'full'
        s3_key = f'{self.file_system}/{backup_time}.{backup_type}'

        backup_db.create_backup(backup_time, backup_type, s3_key)
        backup_db.get_backup(backup_time)

        # When
        backup_db.delete_backup(backup_time)

        # Then
        self.assertRaises(KeyError, backup_db.get_backup, backup_time)

    def test_existing_backup(self):
        """ Test create existing backup. """
        # Given
        backup_db = BackupDB(self.bucket, self.file_system)
        backup_time = '20210425_201838'
        backup_type = 'full'
        s3_key = f'{self.file_system}/{backup_time}.{backup_type}'

        # When
        backup_db.create_backup(backup_time, backup_type, s3_key)

        # Then
        self.assertRaises(ValueError, backup_db.create_backup, backup_time,
                          backup_type, s3_key)

    def test_bad_backup_time(self):
        # Given
        backup_db = BackupDB(self.bucket, self.file_system)
        backup_time = '20210425-201838'
        backup_type = 'full'
        s3_key = f'{self.file_system}/{backup_time}.{backup_type}'

        # Then
        self.assertRaises(ValueError, backup_db.create_backup, backup_time,
                          backup_type, s3_key)

    def test_bad_backup_type(self):
        # Given
        backup_db = BackupDB(self.bucket, self.file_system)
        backup_time = '20210425_201838'
        backup_type = 'badtype'
        s3_key = f'{self.file_system}/{backup_time}.{backup_type}'

        # Then
        self.assertRaises(ValueError, backup_db.create_backup, backup_time,
                          backup_type, s3_key)
