from datetime import datetime
from io import BytesIO
import json

from botocore.exceptions import ClientError

from zfs_uploader import BACKUP_DB_FILE, DATETIME_FORMAT


class BackupDB:
    """ Backup DB object. """

    @property
    def file_system(self):
        """ ZFS filesystem. """
        return self._file_system

    def __init__(self, bucket, file_system):
        self._file_system = file_system
        self._backups = {}
        self._s3_object = bucket.Object(
            f'{self._file_system}/{BACKUP_DB_FILE}')

        # initialize from backup.db file if it exists
        self.download()

    def create_backup(self, backup_time, backup_type, s3_key,
                      dependency=None):
        """ Create backup and upload `backup.db`. """
        if backup_time in self._backups:
            raise ValueError('Backup already exists.')

        if dependency not in self._backups:
            raise ValueError('Depending on backup does not exist.')

        self._backups.update({
            backup_time: Backup(backup_time, backup_type, self._file_system,
                                s3_key, dependency)
        })

        self.upload()

    def delete_backup(self, backup_time):
        """ Delete backup and upload `backup.db`. """
        if _validate_backup_time(backup_time) is False:
            raise ValueError('backup_time is wrong format')

        del self._backups[backup_time]

        self.upload()

    def get_backup(self, backup_time):
        """ Get backup using backup time. """
        if _validate_backup_time(backup_time) is False:
            raise ValueError('backup_time is wrong format')

        try:
            return self._backups[backup_time]
        except KeyError:
            raise KeyError('Backup does not exist.') from None

    def get_sorted_backup_times(self, reverse=False):
        """ Get list of sorted backup times. """
        return sorted(self._backups, reverse=reverse)

    def download(self):
        """ Download backup.db file. """
        try:
            with BytesIO() as f:
                self._s3_object.download_fileobj(f)
                f.seek(0)
                self._backups = json.load(f, object_hook=_json_object_hook)
        except ClientError:
            pass

    def upload(self):
        """ Upload backup.db file. """
        with BytesIO() as f:
            json_str = json.dumps(self._backups, default=_json_default)
            f.write(json_str.encode('utf-8'))
            f.seek(0)
            self._s3_object.upload_fileobj(f)


class Backup:
    """ Backup object. """

    @property
    def backup_time(self):
        """ Backup time. """
        return self._backup_time

    @property
    def backup_type(self):
        """ Backup type. """
        return self._backup_type

    @property
    def file_system(self):
        """ ZFS filesystem. """
        return self._file_system

    @property
    def s3_key(self):
        """ S3 key. """
        return self._s3_key

    @property
    def dependency(self):
        """ Backup time of dependency. """
        return self._dependency

    def __init__(self, backup_time, backup_type, file_system, s3_key,
                 dependency=None):
        if _validate_backup_time(backup_time):
            self._backup_time = backup_time
        else:
            raise ValueError('backup_time is wrong format')

        if backup_type in ['full', 'inc']:
            self._backup_type = backup_type
        else:
            raise ValueError('backup_type must be `full` or `inc`')

        self._file_system = file_system
        self._s3_key = s3_key

        if dependency and _validate_backup_time(dependency):
            self._dependency = dependency
        else:
            raise ValueError('dependency is wrong format')

    def __eq__(self, other):
        return all((self._backup_time == other._backup_time,
                    self._backup_type == other._backup_type,
                    self._file_system == other._file_system,
                    self._s3_key == other._s3_key,
                    self._dependency == other._dependency
                    ))

    def __hash__(self):
        return hash((self._backup_time,
                     self._backup_type,
                     self._file_system,
                     self._s3_key,
                     self._dependency
                     ))


def _json_default(obj):
    if isinstance(obj, Backup):
        return {
            '_type': 'Backup',
            'backup_time': obj._backup_time,
            'backup_type': obj._backup_type,
            'file_system': obj._file_system,
            's3_key': obj._s3_key,
            'dependency': obj._dependency
        }


def _json_object_hook(dct):
    obj_type = dct.get('_type')
    if obj_type == 'Backup':
        dct_copy = dct.copy()
        del dct_copy['_type']

        return Backup(**dct_copy)
    else:
        return dct


def _validate_backup_time(backup_time):
    try:
        datetime.strptime(backup_time, DATETIME_FORMAT)
    except ValueError:
        return False

    return True