from configparser import ConfigParser


class Config:
    """ Wrapper for configuration file. """
    @property
    def bucket(self):
        """ S3 bucket. """
        return self.cfg['global']['bucket']

    @property
    def region(self):
        """ S3 region. """
        return self.cfg['global'].get('region', 'us-east-1')

    @property
    def access_key(self):
        """ S3 access key. """
        return self.cfg['global']['access_key']

    @property
    def secret_key(self):
        """ S3 secret key. """
        return self.cfg['global']['secret_key']

    def __init__(self):
        self.cfg = ConfigParser()
        self.cfg.read('config.cfg')
