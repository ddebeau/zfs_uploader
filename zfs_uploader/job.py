from datetime import datetime

from zfs_uploader.upload import get_s3_resource
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

        self._s3 = get_s3_resource(self._region, self._access_key,
                                   self._secret_key)

    def start(self):
        """ Start ZFS backup job. """
        backup_info = self._get_backup_info()

        if backup_info:
            backup_full = None
            for item in backup_info:
                # find the most recent full backup
                if item['type'] == 'full':
                    backup_full = item['datetime'].strftime(DATETIME_FORMAT)
                    break

            if backup_full:
                self._backup_incremental(backup_full)
            else:
                self._backup_full()

        else:
            self._backup_full()

    def _get_backup_info(self):
        bucket = self._s3.Bucket(self._bucket)
        keys = [item.key for item in bucket.objects.all()]

        backup_info = []
        for item in keys:
            if self._filesystem in item:
                snapshot = item.split('/')[-1]
                date_str, backup_type = snapshot.split('.')
                backup_info.append(
                    {'datetime': datetime.strptime(date_str, DATETIME_FORMAT),
                     'type': backup_type})

        return sorted(backup_info, key=lambda x: x['datetime'], reverse=True)

    def _backup_full(self):
        snapshot = self._create_snapshot()
        backup = f'{self._filesystem}/{snapshot}.full'
        bucket = self._s3.Bucket(self._bucket)
        with open_snapshot_stream(self.filesystem, snapshot, 'r') as f:
            bucket.upload_fileobj(f.stdout, backup)
            stderr = f.stderr.read().decode('utf-8')
        if f.returncode:
            raise ZFSError(stderr)

        self._check_backup(backup)

    def _backup_incremental(self, snapshot_1):
        snapshot_2 = self._create_snapshot()
        backup = f'{self._filesystem}/{snapshot_2}.inc'
        bucket = self._s3.Bucket(self._bucket)
        with open_snapshot_stream_inc(
                self.filesystem, snapshot_1, snapshot_2) as f:
            bucket.upload_fileobj(f.stdout, backup)
            stderr = f.stderr.read().decode('utf-8')
        if f.returncode:
            raise ZFSError(stderr)

        self._check_backup(backup)

    def _create_snapshot(self):
        snapshot = _get_date_time()
        out = create_snapshot(self.filesystem, snapshot)
        if out.returncode:
            raise ZFSError(out.stderr)

        return snapshot

    def _check_backup(self, backup):
        # load() will fail if object does not exist
        backup_object = self._s3.Object(self._bucket, backup)
        backup_object.load()
        if backup_object.content_length == 0:
            raise BackupError('Backup upload failed.')


def _get_date_time():
    return datetime.now().strftime(DATETIME_FORMAT)
