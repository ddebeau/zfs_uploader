from datetime import datetime
import logging
from time import sleep

import boto3
from boto3.s3.transfer import TransferConfig

from zfs_uploader import BACKUP_DB_FILE, DATETIME_FORMAT
from zfs_uploader.backup_db import BackupDB
from zfs_uploader.zfs import (create_snapshot, destroy_snapshot,
                              list_snapshots, open_snapshot_stream,
                              open_snapshot_stream_inc, ZFSError)


class BackupError(Exception):
    """ Baseclass for backup exceptions. """


class ZFSjob:
    """ ZFS backup job. """
    @property
    def bucket(self):
        """ S3 bucket. """
        return self.bucket

    @property
    def region(self):
        """ S3 region. """
        return self._region

    @property
    def access_key(self):
        """ S3 access key. """
        return self._access_key

    @property
    def secret_key(self):
        """ S3 secret key. """
        return self._secret_key

    @property
    def filesystem(self):
        """ ZFS filesystem. """
        return self._file_system

    @property
    def s3(self):
        """ S3 resource. """
        return self._s3

    @property
    def cron(self):
        """ Cron schedule. """
        return self._cron

    @property
    def max_snapshots(self):
        """ Maximum number of snapshots. """
        return self._max_snapshots

    @property
    def max_incremental_backups(self):
        """ Maximum number of incremental backups. """
        return self._max_incremental_backups

    @property
    def storage_class(self):
        """ S3 storage class. """
        return self._storage_class

    def __init__(self, bucket_name, access_key, secret_key, file_system,
                 region='us-east-1', cron=None, max_snapshots=None,
                 max_incremental_backups=None, storage_class='STANDARD'):
        """ Construct ZFS backup job. """
        self._bucket_name = bucket_name
        self._region = region
        self._access_key = access_key
        self._secret_key = secret_key
        self._file_system = file_system

        self._s3 = boto3.resource(service_name='s3',
                                  region_name=self._region,
                                  aws_access_key_id=self._access_key,
                                  aws_secret_access_key=self._secret_key)
        self._s3_transfer_config = TransferConfig(max_concurrency=20)
        self._bucket = self._s3.Bucket(self._bucket_name)
        self._backup_db = BackupDB(self._bucket, self._file_system)
        self._cron = cron
        self._max_snapshots = max_snapshots
        self._max_incremental_backups = max_incremental_backups
        self._storage_class = storage_class
        self._logger = logging.getLogger(__name__)

    def start(self):
        """ Start ZFS backup job. """
        self._logger.info(f'[{self._file_system}] Starting job.')

        # find most recent full backup
        result = self._backup_db.get_backups(backup_type='full')
        backup_full = result[-1] if result else None

        # run incremental backup if full backup exists
        if backup_full:
            self._backup_incremental(backup_full.backup_time)
        else:
            self._backup_full()

        if self._max_snapshots:
            self._limit_snapshots()
        if self._max_incremental_backups:
            self._limit_incremental_backups()

        self._logger.info(f'[{self._file_system}] Finished job.')

    def restore(self):
        """ Restore from most recent backup. """
        result = self._backup_db.get_backups()

        if result:
            backup = result[-1]
            backup_type = backup.backup_type

            if backup_type == 'full':
                self._restore_snapshot(backup)

            elif backup_type == 'inc':
                # restore full backup first
                backup_time_full = backup.dependency.backup_time
                self._restore_snapshot(backup_time_full)
                self._restore_snapshot(backup)
        else:
            print(f'No {BACKUP_DB_FILE} file exists.')

    def _backup_full(self):
        backup_time = self._create_snapshot()
        s3_key = f'{self._file_system}/{backup_time}.full'
        self._logger.info(f'[{s3_key}] Starting full backup.')

        with open_snapshot_stream(self.filesystem, backup_time, 'r') as f:
            self._bucket.upload_fileobj(f.stdout,
                                        s3_key,
                                        Config=self._s3_transfer_config,
                                        ExtraArgs={
                                            'StorageClass': self._storage_class
                                        })
            stderr = f.stderr.read().decode('utf-8')
        if f.returncode:
            raise ZFSError(stderr)

        self._check_backup(s3_key)
        self._backup_db.create_backup(backup_time, 'full', s3_key)
        self._logger.info(f'[{self._file_system}] Finished full backup.')

    def _backup_incremental(self, backup_time_full):
        backup_time = self._create_snapshot()
        s3_key = f'{self._file_system}/{backup_time}.inc'
        self._logger.info(f'[{s3_key}] Starting incremental backup.')

        with open_snapshot_stream_inc(
                self.filesystem, backup_time_full, backup_time) as f:
            self._bucket.upload_fileobj(
                f.stdout,
                s3_key,
                Config=self._s3_transfer_config,
                ExtraArgs={
                    'StorageClass': self._storage_class
                })
            stderr = f.stderr.read().decode('utf-8')
        if f.returncode:
            raise ZFSError(stderr)

        self._check_backup(s3_key)
        self._backup_db.create_backup(backup_time, 'inc', s3_key,
                                      backup_time_full)
        self._logger.info(f'[{self._file_system}] Finished incremental backup.')

    def _restore_snapshot(self, backup):
        backup_time = backup.backup_time
        file_system = backup.file_system
        s3_key = backup.s3_key

        self._logger.info(f'[{file_system}@{backup_time}] Restoring snapshot.')
        backup_object = self._s3.Object(self._bucket_name, s3_key)

        with open_snapshot_stream(self.filesystem, backup_time, 'w') as f:
            backup_object.download_fileobj(f.stdin,
                                           Config=self._s3_transfer_config)
            stderr = f.stderr.read().decode('utf-8')
        if f.returncode:
            raise ZFSError(stderr)

    def _create_snapshot(self):
        backup_time = _get_date_time()
        snapshot_name = f'{self._file_system}@{backup_time}'

        if snapshot_name in list_snapshots():
            # sleep for one second in order to increment snapshot_time
            sleep(1)
            backup_time = _get_date_time()
            snapshot_name = f'{self._file_system}@{backup_time}'

        self._logger.info(f'[{snapshot_name}] Creating snapshot.')
        out = create_snapshot(self._file_system, backup_time)
        if out.returncode:
            raise ZFSError(out.stderr)

        return backup_time

    def _limit_snapshots(self):
        backup_times = self._backup_db.get_backup_times()
        snapshot_keys = [key for key in list_snapshots().keys() if
                         self._file_system in key]

        if len(snapshot_keys) > self._max_snapshots:
            self._logger.info(f'[{self._file_system}] Snapshot limit achieved.')

        while len(snapshot_keys) > self._max_snapshots:
            snapshot = snapshot_keys.pop(0)
            filesystem = snapshot.split('@')[0]
            backup_time = snapshot.split('@')[1]

            if backup_time not in backup_times:
                self._logger.info(f'[{snapshot}] Deleting snapshot.')
                destroy_snapshot(filesystem, backup_time)

    def _check_backup(self, key):
        # load() will fail if object does not exist
        backup_object = self._s3.Object(self._bucket_name, key)
        backup_object.load()
        if backup_object.content_length == 0:
            raise BackupError('Backup upload failed.')

    def _delete_backup(self, backup):
        backup_time = backup.backup_time
        s3_key = backup.s3_key

        self._logger.info(f'[{s3_key}] Deleting backup.')
        backup_object = self._s3.Object(self._bucket_name, s3_key)
        backup_object.delete()
        self._backup_db.delete_backup(backup_time)

    def _limit_incremental_backups(self):
        backups_inc = self._backup_db.get_backups(backup_type='inc')

        if backups_inc:
            if len(backups_inc) > self._max_incremental_backups:
                self._logger.info(f'[{self._file_system}] Incremental backup '
                                  f'limit achieved.')

            while len(backups_inc) > self._max_incremental_backups:
                backup = backups_inc.pop(0)
                self._delete_backup(backup)


def _get_date_time():
    return datetime.now().strftime(DATETIME_FORMAT)
