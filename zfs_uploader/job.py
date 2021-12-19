import logging
import time
import sys

import boto3
from boto3.s3.transfer import TransferConfig

from zfs_uploader.backup_db import BackupDB
from zfs_uploader.snapshot_db import SnapshotDB
from zfs_uploader.zfs import (create_filesystem, get_snapshot_send_size,
                              get_snapshot_send_size_inc,
                              open_snapshot_stream,
                              open_snapshot_stream_inc, ZFSError)

KB = 1024
MB = KB * KB
S3_MAX_CONCURRENCY = 20
S3_MAX_PART_NUMBER = 10000


class BackupError(Exception):
    """ Baseclass for backup exceptions. """


class RestoreError(Exception):
    """ Baseclass for restore exceptions. """


class ZFSjob:
    """ ZFS backup job. """
    @property
    def bucket(self):
        """ S3 bucket. """
        return self._bucket

    @property
    def region(self):
        """ S3 region. """
        return self._region

    @property
    def endpoint(self):
        """ S3 Endpoint. """
        return self._endpoint

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
    def max_backups(self):
        """ Maximum number of full and incremental backups. """
        return self._max_backups

    @property
    def max_incremental_backups_per_full(self):
        """ Maximum number of incremental backups per full backup. """
        return self._max_incremental_backups_per_full

    @property
    def storage_class(self):
        """ S3 storage class. """
        return self._storage_class

    @property
    def backup_db(self):
        """ BackupDB """
        return self._backup_db

    @property
    def snapshot_db(self):
        """ SnapshotDB """
        return self._snapshot_db

    def __init__(self, bucket_name, access_key, secret_key, filesystem,
                 region=None, cron=None, max_snapshots=None, max_backups=None,
                 max_incremental_backups_per_full=None, storage_class=None,
                 endpoint=None):
        """ Create ZFSjob object.

        Parameters
        ----------
        bucket_name : str
            S3 bucket name.
        access_key : str
            S3 access key.
        secret_key : str
            S3 secret key.
        filesystem : str
            ZFS filesystem.
        region : str, default: us-east-1
            S3 region.
        endpoint : str, optional
            S3 endpoint for alternative services
        cron : str, optional
            Cron schedule. Example: `* 0 * * *`
        max_snapshots : int, optional
            Maximum number of snapshots.
        max_backups : int, optional
            Maximum number of full and incremental backups.
        max_incremental_backups_per_full : int, optional
            Maximum number of incremental backups per full backup.
        storage_class : str, default: STANDARD
            S3 storage class.

        """
        self._bucket_name = bucket_name
        self._region = region or 'us-east-1'
        self._access_key = access_key
        self._secret_key = secret_key
        self._filesystem = filesystem
        self._endpoint = endpoint

        self._s3 = boto3.resource(service_name='s3',
                                  region_name=self._region,
                                  aws_access_key_id=self._access_key,
                                  aws_secret_access_key=self._secret_key,
                                  endpoint_url=endpoint)
        self._bucket = self._s3.Bucket(self._bucket_name)
        self._backup_db = BackupDB(self._bucket, self._filesystem)
        self._snapshot_db = SnapshotDB(self._filesystem)
        self._cron = cron
        self._max_snapshots = max_snapshots
        self._max_backups = max_backups
        self._max_incremental_backups_per_full = max_incremental_backups_per_full # noqa
        self._storage_class = storage_class or 'STANDARD'
        self._logger = logging.getLogger(__name__)

        if max_snapshots and not max_snapshots >= 0:
            self._logger.error(f'filesystem={self._filesystem} '
                               'msg="max_snapshots must be greater than or '
                               'equal to 0."')
            sys.exit(1)

        if max_backups and not max_backups >= 1:
            self._logger.error(f'filesystem={self._filesystem} '
                               'msg="max_backups must be greater '
                               'than or equal to 1."')
            sys.exit(1)

        if max_incremental_backups_per_full and not max_incremental_backups_per_full >= 0: # noqa
            self._logger.error(f'filesystem={self._filesystem} '
                               'msg="max_incremental_backups_per_full must be '
                               'greater than or equal to 0."')
            sys.exit(1)

    def start(self):
        """ Start ZFS backup job. """
        self._logger.info(f'filesystem={self._filesystem} msg="Starting job."')
        backups_inc = self._backup_db.get_backups(backup_type='inc')
        backups_full = self._backup_db.get_backups(backup_type='full')

        # find most recent full backup
        backup = backups_full[-1] if backups_full else None

        # if no full backup exists
        if backup is None:
            self._backup_full()

        # if we don't want incremental backups
        elif self._max_incremental_backups_per_full == 0:
            self._backup_full()

        # if we want incremental backups and multiple full backups
        elif self._max_incremental_backups_per_full:
            backup_time = backup.backup_time

            dependants = [True if b.dependency == backup_time
                          else False for b in backups_inc]

            if sum(dependants) >= self._max_incremental_backups_per_full:
                self._backup_full()
            else:
                self._backup_incremental(backup_time)

        # if we want incremental backups and not multiple full backups
        else:
            self._backup_incremental(backup.backup_time)

        if self._max_snapshots or self._max_snapshots == 0:
            self._limit_snapshots()
        if self._max_backups or self._max_backups == 0:
            self._limit_backups()

        self._logger.info(f'filesystem={self._filesystem} msg="Finished job."')

    def restore(self, backup_time=None, filesystem=None):
        """ Restore from backup.

        Defaults to most recent backup if backup_time is not specified.

        WARNING: If restoring to a file system that already exists, snapshots
        and data that were written after the backup will be destroyed.

        Parameters
        ----------
        backup_time : str, optional
            Backup time in %Y%m%d_%H%M%S format.

        filesystem : str, optional
            File system to restore to. Defaults to the file system that the
            backup was taken from.
        """
        self._snapshot_db.refresh()
        snapshots = self._snapshot_db.get_snapshot_names()

        if backup_time:
            backup = self._backup_db.get_backup(backup_time)
        else:
            backups = self._backup_db.get_backups()
            if backups is None:
                raise RestoreError('No backups exist.')
            else:
                backup = backups[-1]

        backup_time = backup.backup_time
        backup_type = backup.backup_type
        s3_key = backup.s3_key

        if filesystem:
            out = create_filesystem(filesystem)
            if out.returncode:
                raise ZFSError(out.stderr)

        if backup_type == 'full':
            if backup_time in snapshots and filesystem is None:
                self._logger.info(f'filesystem={self.filesystem} '
                                  f'snapshot_name={backup_time} '
                                  f's3_key={s3_key} '
                                  'msg="Snapshot already exists."')
            else:
                self._restore_snapshot(backup, filesystem)

        elif backup_type == 'inc':
            # restore full backup first
            backup_full = self._backup_db.get_backup(backup.dependency)

            if backup_full.backup_time in snapshots and filesystem is None:
                self._logger.info(f'filesystem={self.filesystem} '
                                  f'snapshot_name={backup_full.backup_time} '
                                  f's3_key={backup_full.s3_key} '
                                  'msg="Snapshot already exists."')
            else:
                self._restore_snapshot(backup_full, filesystem)

            if backup_time in snapshots and filesystem is None:
                self._logger.info(f'filesystem={self.filesystem} '
                                  f'snapshot_name={backup_time} '
                                  f's3_key={s3_key} '
                                  'msg="Snapshot already exists."')
            else:
                self._restore_snapshot(backup, filesystem)

    def _backup_full(self):
        """ Create snapshot and upload full backup. """
        snapshot = self._snapshot_db.create_snapshot()
        backup_time = snapshot.name
        filesystem = snapshot.filesystem

        send_size = int(get_snapshot_send_size(filesystem, backup_time))
        transfer_config = _get_transfer_config(send_size)

        s3_key = f'{filesystem}/{backup_time}.full'
        self._logger.info(f'filesystem={filesystem} '
                          f'snapshot_name={backup_time} '
                          f's3_key={s3_key} '
                          'msg="Starting full backup."')

        with open_snapshot_stream(filesystem, backup_time, 'r') as f:
            transfer_callback = TransferCallback(self._logger, send_size,
                                                 filesystem, backup_time,
                                                 s3_key)
            self._bucket.upload_fileobj(f.stdout,
                                        s3_key,
                                        Callback=transfer_callback.callback,
                                        Config=transfer_config,
                                        ExtraArgs={
                                            'StorageClass': self._storage_class
                                        })
            stderr = f.stderr.read().decode('utf-8')
        if f.returncode:
            raise ZFSError(stderr)

        backup_size = self._check_backup(s3_key)
        self._backup_db.create_backup(backup_time, 'full', s3_key,
                                      dependency=None, backup_size=backup_size)
        self._logger.info(f'filesystem={filesystem} '
                          f'snapshot_name={backup_time} '
                          f's3_key={s3_key} '
                          'msg="Finished full backup."')

    def _backup_incremental(self, backup_time_full):
        """ Create snapshot and upload incremental backup.

        Parameters
        ----------
        backup_time_full : str
            Backup time in %Y%m%d_%H%M%S format.

        """
        snapshot = self._snapshot_db.create_snapshot()
        backup_time = snapshot.name
        filesystem = snapshot.filesystem

        send_size = int(get_snapshot_send_size_inc(filesystem,
                                                   backup_time_full,
                                                   backup_time))
        transfer_config = _get_transfer_config(send_size)

        s3_key = f'{filesystem}/{backup_time}.inc'
        self._logger.info(f'filesystem={filesystem} '
                          f'snapshot_name={backup_time} '
                          f's3_key={s3_key} '
                          'msg="Starting incremental backup."')

        with open_snapshot_stream_inc(
                filesystem, backup_time_full, backup_time) as f:
            transfer_callback = TransferCallback(self._logger, send_size,
                                                 filesystem, backup_time,
                                                 s3_key)
            self._bucket.upload_fileobj(
                f.stdout,
                s3_key,
                Callback=transfer_callback.callback,
                Config=transfer_config,
                ExtraArgs={
                    'StorageClass': self._storage_class
                })
            stderr = f.stderr.read().decode('utf-8')
        if f.returncode:
            raise ZFSError(stderr)

        backup_size = self._check_backup(s3_key)
        self._backup_db.create_backup(backup_time, 'inc', s3_key,
                                      backup_time_full, backup_size)
        self._logger.info(f'filesystem={filesystem} '
                          f'snapshot_name={backup_time} '
                          f's3_key={s3_key} '
                          'msg="Finished incremental backup."')

    def _restore_snapshot(self, backup, filesystem=None):
        """ Restore snapshot from backup.

        Parameters
        ----------
        backup : Backup

        filesystem : str, optional
            File system to restore to. Defaults to the file system that the
            backup was taken from.
        """
        backup_time = backup.backup_time
        backup_size = backup.backup_size
        filesystem = filesystem or backup.filesystem
        s3_key = backup.s3_key

        transfer_config = TransferConfig(max_concurrency=S3_MAX_CONCURRENCY)

        self._logger.info(f'filesystem={filesystem} '
                          f'snapshot_name={backup_time} '
                          f's3_key={s3_key} '
                          'msg="Restoring snapshot."')
        backup_object = self._s3.Object(self._bucket_name, s3_key)

        with open_snapshot_stream(filesystem, backup_time, 'w') as f:
            transfer_callback = TransferCallback(self._logger, backup_size,
                                                 filesystem, backup_time,
                                                 s3_key)
            try:
                backup_object.download_fileobj(
                    f.stdin,
                    Callback=transfer_callback.callback,
                    Config=transfer_config)
            except BrokenPipeError:
                pass
            stderr = f.stderr.read().decode('utf-8')
        if f.returncode:
            raise ZFSError(stderr)

        self._snapshot_db.refresh()

    def _limit_snapshots(self):
        """ Limit number of snapshots.

        We only remove snapshots that were used for incremental backups.
        Keeping snapshots that were used for full backups allow us to
        restore without having to download the full backup.
        """
        backup_times_full = self._backup_db.get_backup_times('full')
        results = self._snapshot_db.get_snapshots()

        if len(results) > self._max_snapshots:
            self._logger.info(f'filesystem={self._filesystem} '
                              'msg="Snapshot limit achieved."')

        while len(results) > self._max_snapshots:
            snapshot = results.pop(0)
            backup_time = snapshot.name

            if backup_time not in backup_times_full:
                self._logger.info(f'filesystem={self._filesystem} '
                                  f'snapshot_name={snapshot.name} '
                                  'msg="Deleting snapshot."')
                self._snapshot_db.delete_snapshot(snapshot.name)

    def _check_backup(self, s3_key):
        """ Check if S3 object exists and returns object size.

        Parameters
        ----------
        s3_key : str

        Returns
        -------
        int

        """
        # load() will fail if object does not exist
        backup_object = self._s3.Object(self._bucket_name, s3_key)
        backup_object.load()
        if backup_object.content_length == 0:
            raise BackupError('Backup upload failed.')

        return backup_object.content_length

    def _delete_backup(self, backup):
        """ Delete backup.

        Parameters
        ----------
        backup : Backup

        """
        backup_time = backup.backup_time
        s3_key = backup.s3_key

        self._logger.info(f's3_key={s3_key} '
                          'msg="Deleting backup."')
        backup_object = self._s3.Object(self._bucket_name, s3_key)
        backup_object.delete()
        self._backup_db.delete_backup(backup_time)

    def _limit_backups(self):
        """ Limit number of incremental and full backups.

        Only backups with no dependants are removed.
        """
        backups = self._backup_db.get_backups()

        if len(backups) > self._max_backups:
            self._logger.info(f'filesystem={self._filesystem} '
                              'msg="Backup limit achieved."')

        count = 0
        while len(backups) > self._max_backups:
            backup = backups[count]
            backup_time = backup.backup_time
            backup_type = backup.backup_type
            s3_key = backup.s3_key

            if backup_type == "inc":
                self._delete_backup(backup)
                backups.pop(count)

            elif backup_type == "full":
                dependants = any([True if b.dependency == backup_time
                                  else False for b in backups])
                if dependants:
                    self._logger.info(f's3_key={s3_key} '
                                      'msg="Backup has dependants. Not '
                                      'deleting."')
                else:
                    self._delete_backup(backup)
                    backups.pop(count)

            count += 1


class TransferCallback:
    def __init__(self, logger, file_size, filesystem, backup_time, s3_key):
        self._logger = logger
        self._file_size = file_size
        self._filesystem = filesystem
        self._backup_time = backup_time
        self._s3_key = s3_key

        self._transfer_0 = 0
        self._transfer_buffer = 0
        self._time_0 = time.time()
        self._time_start = time.time()

    def callback(self, transfer):
        time_1 = time.time()
        time_diff = time_1 - self._time_0
        time_elapsed = time_1 - self._time_start

        self._transfer_buffer += transfer

        if time_diff > 5:
            transfer_1 = self._transfer_0 + self._transfer_buffer

            progress = transfer_1 / self._file_size
            speed = self._transfer_buffer / time_diff

            self._logger.info(
                f'filesystem={self._filesystem} '
                f'snapshot_name={self._backup_time} '
                f's3_key={self._s3_key} '
                f'progress={round(progress * 100)}% '
                f'speed="{round(speed / MB)} MBps" '
                f'transferred="{round(transfer_1 / MB)}/'
                f'{round(self._file_size / MB)} MB" '
                f'time_elapsed={round(time_elapsed / 60)}m'
            )

            self._transfer_0 = transfer_1
            self._transfer_buffer = 0
            self._time_0 = time_1


def _get_transfer_config(send_size):
    """ Get transfer config. """
    # should never get close to the max part number
    chunk_size = send_size // (S3_MAX_PART_NUMBER - 100)
    # only set chunk size if greater than default value
    chunk_size = chunk_size if chunk_size > 8 * MB else 8 * MB
    return TransferConfig(max_concurrency=S3_MAX_CONCURRENCY,
                          multipart_chunksize=chunk_size)
