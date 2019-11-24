from io import BytesIO
import unittest
import warnings

from zfs_uploader.config import Config


class UploadTests(unittest.TestCase):
    def setUp(self):
        warnings.filterwarnings("ignore", category=ResourceWarning,
                                message="unclosed.*<ssl.SSLSocket.*>")

        config = Config('../config.cfg')
        self.job = next(iter(config.jobs.values()))
        self.bucket = self.job.bucket
        self.key = 'temp'

    def tearDown(self):
        self.bucket.delete_objects(Delete={'Objects': [{'Key': self.key}]})

    def test_fileobj_upload(self):
        """ Test fileobj upload to S3. """
        # Given
        test_string = b'hello world'

        with BytesIO(test_string) as f:
            self.bucket.upload_fileobj(f, self.key)

        # When
        with BytesIO() as f:
            self.bucket.download_fileobj(self.key, f)
            f.seek(0)
            out = f.read()

        # Then
        self.assertEqual(test_string, out)
