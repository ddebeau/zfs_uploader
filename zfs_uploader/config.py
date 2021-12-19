from configparser import ConfigParser
import logging
import os
import sys

from zfs_uploader.job import ZFSjob


class Config:
    """ Wrapper for configuration file. """

    @property
    def jobs(self):
        """ ZFS backup jobs. """
        return self._jobs

    def __init__(self, file_path=None):
        """ Construct Config object from file.

        Parameters
        ----------
        file_path : str
            File path to config file.

        """
        file_path = file_path or 'config.cfg'

        self._logger = logging.getLogger(__name__)
        self._logger.info(f'file_path={file_path} '
                          'msg="Loading configuration file."')

        if not os.path.isfile(file_path):
            self._logger.critical('No configuration file found.')
            sys.exit(1)

        self._cfg = ConfigParser()
        self._cfg.read(file_path)

        default = self._cfg['DEFAULT']
        self._jobs = {}
        for k, v in self._cfg.items():
            if k != 'DEFAULT':
                bucket_name = (v.get('bucket_name') or
                               default.get('bucket_name'))
                access_key = v.get('access_key') or default.get('access_key')
                secret_key = v.get('secret_key') or default.get('secret_key')
                filesystem = k

                if not all((bucket_name, access_key, secret_key)):
                    self._logger.critical(f'file_path={file_path} '
                                          f'filesystem={filesystem}'
                                          'msg="bucket_name, access_key or '
                                          'secret_key is missing from config."'
                                          )
                    sys.exit(1)

                cron_dict = None
                cron = v.get('cron') or default.get('cron')
                if cron:
                    cron_dict = _create_cron_dict(cron)

                self._jobs[k] = (
                    ZFSjob(
                        bucket_name,
                        access_key,
                        secret_key,
                        filesystem,
                        region=v.get('region') or default.get('region'),
                        endpoint=v.get('endpoint') or default.get('endpoint'),
                        cron=cron_dict,
                        max_snapshots=(v.getint('max_snapshots') or
                                       default.getint('max_snapshots')),
                        max_backups=(
                                v.getint('max_backups') or
                                default.getint('max_backups')),
                        max_incremental_backups_per_full=(
                                v.getint('max_incremental_backups_per_full') or
                                default.getint('max_incremental_backups_per_full')), # noqa
                        storage_class=(v.get('storage_class') or
                                       default.get('storage_class'))
                    )
                )


def _create_cron_dict(cron):
    values = cron.split()

    return {'minute': values[0],
            'hour': values[1],
            'day': values[2],
            'month': values[3],
            'day_of_week': values[4]}
