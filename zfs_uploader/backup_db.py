from io import BytesIO
import json

from botocore.exceptions import ClientError

BACKUP_DB_FILE = 'backup.db'


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

        # initialize from backup.info file if it exists
        self.download()

    def create_backup(self, backup_time, backup_type, key):
        if backup_time in self._backups:
            raise ValueError('Backup key already exists.')

        self._backups.update({
            backup_time: Backup(backup_time, backup_type, self._file_system,
                                key)
        })

        self.upload()

    def delete_backup(self, key):
        del self._backups[key]

        self.upload()

    def get_backup(self, key):
        try:
            return self._backups[key]
        except KeyError:
            raise KeyError('Backup key does not exist.') from None

    def get_sorted_keys(self, reverse=False):
        return sorted(self._backups, reverse=reverse)

    def download(self):
        try:
            with BytesIO() as f:
                self._s3_object.download_fileobj(f)
                f.seek(0)
                self._backups = json.load(f, object_hook=_json_object_hook)
        except ClientError:
            pass

    def upload(self):
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

    def __init__(self, backup_time, backup_type, file_system, s3_key):
        self._backup_time = backup_time
        self._backup_type = backup_type
        self._file_system = file_system
        self._s3_key = s3_key

    def __eq__(self, other):
        return all((self._backup_time == other._backup_time,
                    self._backup_type == other._backup_type,
                    self._file_system == other._file_system,
                    self._s3_key == other._s3_key
                    ))

    def __hash__(self):
        return hash((self._backup_time,
                     self._backup_type,
                     self._file_system,
                     self._s3_key
                     ))


def _json_default(obj):
    if isinstance(obj, Backup):
        return {
            '_type': 'Backup',
            'backup_time': obj._backup_time,
            'backup_type': obj._backup_type,
            'file_system': obj._file_system,
            's3_key': obj._s3_key,
        }


def _json_object_hook(dct):
    obj_type = dct.get('_type')
    if obj_type == 'Backup':
        dct_copy = dct.copy()
        del dct_copy['_type']

        return Backup(**dct_copy)
    else:
        return dct
