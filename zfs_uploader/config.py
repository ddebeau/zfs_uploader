from configparser import ConfigParser
import logging
import os

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
        self._logger = logging.getLogger(__name__)
        self._logger.info('Loading configuration file.')

        file_path = file_path or 'config.cfg'
        if not os.path.isfile(file_path):
            raise IOError('No configuration file found.')

        self._cfg = ConfigParser()
        self._cfg.read(file_path)

        default = self._cfg['DEFAULT']
        self._jobs = {}
        for k, v in self._cfg.items():
            if k != 'DEFAULT':
                cron_dict = None
                cron = v.get('cron') or default.get('cron')
                if cron:
                    cron_dict = _create_cron_dict(cron)

                self._jobs[k] = (
                    ZFSjob(
                        v.get('bucket_name') or default.get('bucket_name'),
                        v.get('access_key') or default.get('access_key'),
                        v.get('secret_key') or default.get('secret_key'),
                        file_system=k,
                        region=v.get('region') or default.get('region'),
                        cron=cron_dict,
                        max_snapshots=(v.getint('max_snapshots') or
                                       default.getint('max_snapshots')),
                        max_incremental_backups=(
                                v.getint('max_incremental_backups') or
                                default.getint('max_incremental_backups')),
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
