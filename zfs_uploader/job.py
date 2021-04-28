from datetime import datetime
from io import BytesIO
import json
import logging
from time import sleep

from botocore.exceptions import ClientError
import boto3
from boto3.s3.transfer import TransferConfig

from zfs_uploader import DATETIME_FORMAT
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
        return self._s3.Bucket(self._bucket)

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
        return self._filesystem

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

    def __init__(self, bucket, access_key, secret_key, filesystem,
                 region='us-east-1', cron=None, max_snapshots=None,
                 max_incremental_backups=None, storage_class='STANDARD'):
        """ Construct ZFS backup job. """
        self._bucket = bucket
        self._region = region
        self._access_key = access_key
        self._secret_key = secret_key
        self._filesystem = filesystem

        self._s3 = boto3.resource(service_name='s3',
                                  region_name=self._region,
                                  aws_access_key_id=self._access_key,
                                  aws_secret_access_key=self._secret_key)
        self._s3_transfer_config = TransferConfig(max_concurrency=20)
        self._cron = cron
        self._max_snapshots = max_snapshots
        self._max_incremental_backups = max_incremental_backups
        self._storage_class = storage_class
        self._logger = logging.getLogger(__name__)

    def start(self):
        """ Start ZFS backup job. """
        self._logger.info(f'[{self._filesystem}] Starting job.')
        backup_info = self._read_backup_info()

        if backup_info:
            backup_keys = list(backup_info.keys())
            backup_time = None

            for key in reversed(backup_keys):
                # find the most recent full backup
                backup = backup_info[key]
                if backup['backup_type'] == 'full':
                    backup_time = backup['backup_time']
                    break

            if backup_time:
                self._backup_incremental(backup_time)
            else:
                self._backup_full()

        else:
            self._backup_full()

        if self._max_snapshots:
            self._limit_snapshots()
        if self._max_incremental_backups:
            self._limit_backups()

        self._logger.info(f'[{self._filesystem}] Finished job.')

    def restore(self):
        """ Restore from most recent backup. """
        backup_info = self._read_backup_info()

        if backup_info:
            backup_keys = list(backup_info.keys())
            key_last = backup_keys[-1]

            if backup_info[key_last]['backup_type'] == 'full':
                self._restore_snapshot(key_last)

            elif backup_info[key_last]['backup_type'] == 'inc':
                for key in reversed(backup_keys):
                    # find the most recent full backup
                    backup = backup_info[key]
                    if backup['backup_type'] == 'full':
                        self._restore_snapshot(key)
                        break

                self._restore_snapshot(key_last)
        else:
            print('No backup_info file exists.')

    def _read_backup_info(self):
        info_object = self._s3.Object(self._bucket,
                                      f'{self._filesystem}/backup.info')
        try:
            with BytesIO() as f:
                info_object.download_fileobj(f)
                f.seek(0)
                return json.load(f)

        except ClientError:
            return {}

    def _write_backup_info(self, backup_info):
        info_object = self._s3.Object(self._bucket,
                                      f'{self._filesystem}/backup.info')
        with BytesIO() as f:
            f.write(json.dumps(backup_info).encode('utf-8'))
            f.seek(0)
            info_object.upload_fileobj(f)

    def _set_backup_info(self, key, file_system, backup_time, backup_type):
        backup_info = self._read_backup_info()
        backup_info[key] = {'file_system': file_system,
                            'backup_time': backup_time,
                            'backup_type': backup_type}
        self._write_backup_info(backup_info)

    def _del_backup_info(self, key):
        backup_info = self._read_backup_info()
        backup_info.pop(key)
        self._write_backup_info(backup_info)

    def _backup_full(self):
        backup_time = self._create_snapshot()
        backup = f'{self._filesystem}/{backup_time}.full'
        self._logger.info(f'[{backup}] Starting full backup.')
        bucket = self._s3.Bucket(self._bucket)
        with open_snapshot_stream(self.filesystem, backup_time, 'r') as f:
            bucket.upload_fileobj(f.stdout,
                                  backup,
                                  Config=self._s3_transfer_config,
                                  ExtraArgs={
                                      'StorageClass': self._storage_class
                                  })
            stderr = f.stderr.read().decode('utf-8')
        if f.returncode:
            raise ZFSError(stderr)

        self._check_backup(backup)
        self._set_backup_info(backup, self._filesystem, backup_time, 'full')
        self._logger.info(f'[{self._filesystem}] Finished full backup.')

    def _backup_incremental(self, snapshot_1):
        backup_time = self._create_snapshot()
        backup = f'{self._filesystem}/{backup_time}.inc'
        self._logger.info(f'[{backup}] Starting incremental backup.')
        bucket = self._s3.Bucket(self._bucket)
        with open_snapshot_stream_inc(
                self.filesystem, snapshot_1, backup_time) as f:
            bucket.upload_fileobj(f.stdout,
                                  backup,
                                  Config=self._s3_transfer_config,
                                  ExtraArgs={
                                      'StorageClass': self._storage_class
                                  })
            stderr = f.stderr.read().decode('utf-8')
        if f.returncode:
            raise ZFSError(stderr)

        self._check_backup(backup)
        self._set_backup_info(backup, self._filesystem, backup_time, 'inc')
        self._logger.info(f'[{self._filesystem}] Finished incremental backup.')

    def _restore_snapshot(self, key):
        backup_info = self._read_backup_info()
        backup = backup_info[key]
        backup_time = backup['backup_time']
        self._logger.info(f'[{self._filesystem}@{backup_time}] Restoring '
                          f'snapshot.')
        backup_object = self._s3.Object(self._bucket, key)

        with open_snapshot_stream(self.filesystem, backup_time, 'w') as f:
            backup_object.download_fileobj(f.stdin,
                                           Config=self._s3_transfer_config)
            stderr = f.stderr.read().decode('utf-8')
        if f.returncode:
            raise ZFSError(stderr)

    def _create_snapshot(self):
        backup_time = _get_date_time()
        self._logger.info(f'[{self._filesystem}@{backup_time}] Creating '
                          f'snapshot.')

        if f'{self._filesystem}@{backup_time}' in list_snapshots():
            sleep(1)

        backup_time = _get_date_time()
        out = create_snapshot(self._filesystem, backup_time)
        if out.returncode:
            raise ZFSError(out.stderr)

        return backup_time

    def _limit_snapshots(self):
        backup_info = self._read_backup_info()
        snapshot_keys = [key for key in list_snapshots().keys() if
                         self._filesystem in key]

        if len(snapshot_keys) > self._max_snapshots:
            self._logger.info(f'[{self._filesystem}] Snapshot limit achieved.')

        while len(snapshot_keys) > self._max_snapshots:
            snapshot = snapshot_keys.pop(0)
            filesystem = snapshot.split('@')[0]
            backup_time = snapshot.split('@')[1]
            key = f'{filesystem}/{backup_time}.full'

            if key not in backup_info:
                self._logger.info(f'[{self._filesystem}@{backup_time}] '
                                  f'Deleting snapshot.')
                destroy_snapshot(filesystem, backup_time)

    def _check_backup(self, key):
        # load() will fail if object does not exist
        backup_object = self._s3.Object(self._bucket, key)
        backup_object.load()
        if backup_object.content_length == 0:
            raise BackupError('Backup upload failed.')

    def _delete_backup(self, key):
        self._logger.info(f'[{key}] Deleting backup.')
        backup_object = self._s3.Object(self._bucket, key)
        backup_object.delete()
        self._del_backup_info(key)

    def _limit_backups(self):
        backup_info = self._read_backup_info()

        if backup_info:
            backup_keys = list(backup_info.keys())
            backups_inc = []

            for key in reversed(backup_keys):
                backup = backup_info[key]

                if backup['backup_type'] == 'inc':
                    backups_inc.append(key)
                else:
                    break

            if len(backups_inc) > self._max_incremental_backups:
                self._logger.info(f'[{self._filesystem}] Backup limit '
                                  f'achieved.')

            while len(backups_inc) > self._max_incremental_backups:
                key = backups_inc.pop(-1)
                self._delete_backup(key)


def _get_date_time():
    return datetime.now().strftime(DATETIME_FORMAT)
