from configparser import ConfigParser

from zfs_uploader.job import ZFSjob


class Config:
    """ Wrapper for configuration file. """

    @property
    def jobs(self):
        """ ZFS backup jobs. """
        return self._jobs

    def __init__(self, file_path=None):
        """ Construct Config object from file. """
        file_path = file_path or 'config.cfg'
        self._cfg = ConfigParser()
        self._cfg.read(file_path)

        default = self._cfg['DEFAULT']
        self._jobs = {}
        for k, v in self._cfg.items():
            if k is not 'DEFAULT':
                self._jobs[k] = (
                    ZFSjob(v.get('bucket') or default.get('bucket'),
                           v.get('access_key') or default.get('access_key'),
                           v.get('secret_key') or default.get('secret_key'),
                           filesystem=k,
                           region=v.get('region') or default.get('region')
                           )
                )
