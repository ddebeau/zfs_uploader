import unittest

from zfs_uploader.config import Config
from zfs_uploader.utils import derive_s3_key


class UtilTests(unittest.TestCase):
    def setUp(self):
        # Given
        config = Config('config.cfg')
        job = next(iter(config.jobs.values()))
        self.filesystem = job.filesystem

    def test_derive_s3_key_no_prefix(self):
        """ Test create s3 key without prefix. """
        # When
        object_name = '20210425_201838.full'
        s3_key = derive_s3_key(object_name, self.filesystem)

        # Then
        self.assertEqual(s3_key, f'{self.filesystem}/{object_name}')

    def test_derive_s3_key_with_prefix(self):
        """ Test create s3 key with prefix. """
        # Given
        prefix = 'prefix'

        # When
        object_name = '20210425_201838.full'
        s3_key = derive_s3_key(object_name, self.filesystem, prefix)

        # Then
        self.assertEqual(s3_key, f'{prefix}/{self.filesystem}/{object_name}')
