from datetime import datetime
from io import BytesIO
import json

from botocore.exceptions import ClientError # noqa

from zfs_uploader import BACKUP_DB_FILE, DATETIME_FORMAT


class BackupDB:
    """ Backup DB object. """

    @property
    def filesystem(self):
        """ ZFS filesystem. """
        return self._filesystem

    def __init__(self, bucket, filesystem):
        """ Create BackupDB object.

        BackupDB is used for storing Backup objects. It does not upload
        backups but serves as a database for backup records.

        Parameters
        ----------
        bucket : Bucket
            S3 Bucket.
        filesystem : str
            ZFS filesystem.

        """
        self._filesystem = filesystem
        self._backups = {}
        self._s3_object = bucket.Object(
            f'{self._filesystem}/{BACKUP_DB_FILE}')

        # initialize from backup.db file if it exists
        self.download()

    def create_backup(self, backup_time, backup_type, s3_key,
                      dependency=None, backup_size=None):
        """ Create backup object and upload `backup.db` file.

        Parameters
        ----------
        backup_time : str
            Backup time in %Y%m%d_%H%M%S format.
        backup_type : str
            Supported backup types are `full` and `inc`.
        s3_key : str
            Backup S3 key.
        dependency : str, optional
            Backup time of dependency in %Y%m%d_%H%M%S format. Used for
            storing the dependent full backup for an incremental backup.
        backup_size : int, optional
            Backup size in bytes.

        """
        if backup_time in self._backups:
            raise ValueError('Backup already exists.')

        if dependency and dependency not in self._backups:
            raise ValueError('Depending on backup does not exist.')

        self._backups.update({
            backup_time: Backup(backup_time, backup_type, self._filesystem,
                                s3_key, dependency, backup_size)
        })

        self.upload()

    def delete_backup(self, backup_time):
        """ Delete backup and upload `backup.db`.

        Parameters
        ----------
        backup_time : str
            Backup time in %Y%m%d_%H%M%S format.

        """
        if _validate_backup_time(backup_time) is False:
            raise ValueError('backup_time is wrong format')

        del self._backups[backup_time]

        self.upload()

    def get_backup(self, backup_time):
        """ Get backup using backup time.

        Parameters
        ----------
        backup_time : str
            Backup time in %Y%m%d_%H%M%S format.

        Returns
        -------
        Backup

        """
        if _validate_backup_time(backup_time) is False:
            raise ValueError('backup_time is wrong format')

        try:
            return self._backups[backup_time]
        except KeyError:
            raise KeyError('Backup does not exist.') from None

    def get_backups(self, backup_type=None):
        """ Get sorted list of backups.

        Parameters
        ----------
        backup_type : str, optional
            Supported backup types are `full` and `inc`.

        Returns
        -------
        list(Backup)
            Sorted list of backups. Most recent backup is last.

        """
        backup_times = sorted(self._backups)

        if backup_type in ['full', 'inc']:
            backups = []
            for time in backup_times:
                backup = self._backups[time]

                if backup.backup_type == backup_type:
                    backups.append(backup)
        elif backup_type is None:
            backups = [self._backups[time] for time in backup_times]
        else:
            raise ValueError('backup_type must be `full` or `inc`')

        return backups

    def get_backup_times(self, backup_type=None):
        """ Get sorted list of backup times.

        Parameters
        ----------
        backup_type : str, optional
            Supported backup types are `full` and `inc`.

        Returns
        -------
        list(str)
            Sorted list of backup times. Most recent backup is last.

        """
        if backup_type in ['full', 'inc']:
            backup_times = []
            for time in sorted(self._backups):
                backup = self._backups[time]

                if backup.backup_type == backup_type:
                    backup_times.append(time)
            return backup_times
        elif backup_type is None:
            return sorted(self._backups)
        else:
            raise ValueError('backup_type must be `full` or `inc`')

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
    def filesystem(self):
        """ ZFS filesystem. """
        return self._filesystem

    @property
    def snapshot_name(self):
        """ ZFS snapshot name. """
        return f'{self._filesystem}@{self._backup_time}'

    @property
    def s3_key(self):
        """ S3 key. """
        return self._s3_key

    @property
    def dependency(self):
        """ Backup time of dependency. """
        return self._dependency

    @property
    def backup_size(self):
        """ Backup size in bytes. """
        return self._backup_size

    def __init__(self, backup_time, backup_type, filesystem, s3_key,
                 dependency=None, backup_size=None):
        """ Create Backup object.

        Parameters
        ----------
        backup_time : str
            Backup time in %Y%m%d_%H%M%S format.
        backup_type : str
            Supported backup types are `full` and `inc`.
        filesystem : str
            ZFS filesystem.
        s3_key : str
            Backup S3 key.
        dependency : str, optional
            Backup time of dependency in %Y%m%d_%H%M%S format. Used for
            storing the dependent full backup for an incremental backup.
        backup_size : int, optional
            Backup size in bytes.

        """
        if _validate_backup_time(backup_time):
            self._backup_time = backup_time
        else:
            raise ValueError('backup_time is wrong format')

        if backup_type in ['full', 'inc']:
            self._backup_type = backup_type
        else:
            raise ValueError('backup_type must be `full` or `inc`')

        self._filesystem = filesystem
        self._s3_key = s3_key

        if dependency:
            if not _validate_backup_time(dependency):
                raise ValueError('dependency is wrong format')
        self._dependency = dependency

        self._backup_size = backup_size

    def __eq__(self, other):
        return all((self._backup_time == other._backup_time, # noqa
                    self._backup_type == other._backup_type, # noqa
                    self._filesystem == other._filesystem, # noqa
                    self._s3_key == other._s3_key, # noqa
                    self._dependency == other._dependency, # noqa
                    self._backup_size == other._backup_size # noqa
                    ))

    def __hash__(self):
        return hash((self._backup_time,
                     self._backup_type,
                     self._filesystem,
                     self._s3_key,
                     self._dependency,
                     self._backup_size
                     ))


def _json_default(obj):
    if isinstance(obj, Backup):
        return {
            '_type': 'Backup',
            'backup_time': obj._backup_time, # noqa
            'backup_type': obj._backup_type, # noqa
            'filesystem': obj._filesystem, # noqa
            's3_key': obj._s3_key, # noqa
            'dependency': obj._dependency, # noqa
            'backup_size': obj._backup_size # noqa
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
