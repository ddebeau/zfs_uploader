from datetime import datetime
from io import BytesIO
import json

from botocore.exceptions import ClientError
import boto3

from zfs_uploader.zfs import (create_snapshot, open_snapshot_stream,
                              open_snapshot_stream_inc, ZFSError)

DATETIME_FORMAT = '%Y%m%d_%H%M%S'


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

    def __init__(self, bucket, access_key, secret_key, filesystem,
                 region='us-east-1'):
        """ Construct ZFS backup job. """
        self._bucket = bucket
        self._region = region
        self._access_key = access_key
        self._secret_key = secret_key
        self._filesystem = filesystem

        self._s3 = _get_s3_resource(self._region, self._access_key,
                                    self._secret_key)

    def start(self):
        """ Start ZFS backup job. """
        backup_info = self._get_backup_info()

        if backup_info:
            backup_time = None
            for backup in reversed(backup_info):
                # find the most recent full backup
                if backup['backup_type'] == 'full':
                    backup_time = backup['backup_time']
                    break

            if backup_time:
                self._backup_incremental(backup_time)
            else:
                self._backup_full()

        else:
            self._backup_full()

    def restore(self):
        """ Restore from most recent backup. """
        backup_info = self._get_backup_info()

        if backup_info:
            backup_last = backup_info.pop()

            if backup_last['backup_type'] == 'full':
                self._restore_snapshot(backup_last)

            elif backup_last['backup_type'] == 'inc':
                for backup in reversed(backup_info):
                    # find the most recent full backup
                    if backup['backup_type'] == 'full':
                        self._restore_snapshot(backup)
                        break

                self._restore_snapshot(backup_last)
        else:
            print('No backup_info file exists.')

    def _get_backup_info(self):
        info_object = self._s3.Object(self._bucket, 'backup.info')

        try:
            with BytesIO() as f:
                info_object.download_fileobj(f)
                f.seek(0)
                return json.load(f)

        except ClientError:
            return []

    def _set_backup_info(self, key, file_system, backup_time, backup_type):
        info_object = self._s3.Object(self._bucket, 'backup.info')

        try:
            with BytesIO() as f:
                info_object.download_fileobj(f)
                f.seek(0)
                backup_info = json.load(f)

        except ClientError:
            backup_info = []

        backup_info.append({'key': key,
                            'file_system': file_system,
                            'backup_time': backup_time,
                            'backup_type': backup_type})

        with BytesIO() as f:
            f.write(json.dumps(backup_info).encode('utf-8'))
            f.seek(0)
            info_object.upload_fileobj(f)

    def _backup_full(self):
        backup_time = self._create_snapshot()
        backup = f'{self._filesystem}/{backup_time}.full'
        bucket = self._s3.Bucket(self._bucket)
        with open_snapshot_stream(self.filesystem, backup_time, 'r') as f:
            bucket.upload_fileobj(f.stdout, backup)
            stderr = f.stderr.read().decode('utf-8')
        if f.returncode:
            raise ZFSError(stderr)

        self._check_backup(backup)
        self._set_backup_info(backup, self._filesystem, backup_time, 'full')

    def _backup_incremental(self, snapshot_1):
        backup_time = self._create_snapshot()
        backup = f'{self._filesystem}/{backup_time}.inc'
        bucket = self._s3.Bucket(self._bucket)
        with open_snapshot_stream_inc(
                self.filesystem, snapshot_1, backup_time) as f:
            bucket.upload_fileobj(f.stdout, backup)
            stderr = f.stderr.read().decode('utf-8')
        if f.returncode:
            raise ZFSError(stderr)

        self._check_backup(backup)
        self._set_backup_info(backup, self._filesystem, backup_time, 'inc')

    def _restore_snapshot(self, backup):
        backup_time = backup['backup_time']
        backup_key = backup['key']
        backup_object = self._s3.Object(self._bucket, backup_key)

        with open_snapshot_stream(self.filesystem, backup_time, 'w') as f:
            backup_object.download_fileobj(f.stdin)
            stderr = f.stderr.read().decode('utf-8')
        if f.returncode:
            raise ZFSError(stderr)

    def _create_snapshot(self):
        backup_time = _get_date_time()
        out = create_snapshot(self.filesystem, backup_time)
        if out.returncode:
            raise ZFSError(out.stderr)

        return backup_time

    def _check_backup(self, backup):
        # load() will fail if object does not exist
        backup_object = self._s3.Object(self._bucket, backup)
        backup_object.load()
        if backup_object.content_length == 0:
            raise BackupError('Backup upload failed.')


def _get_date_time():
    return datetime.now().strftime(DATETIME_FORMAT)


def _get_s3_resource(region, access_key, secret_key):
    """ Get s3 resouce. """
    s3 = boto3.resource(service_name='s3',
                        region_name=region,
                        aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key)
    return s3
