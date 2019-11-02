from io import BytesIO
import unittest
import warnings

from zfs_uploader.config import Config
from zfs_uploader.upload import get_s3_client


class MyTestCase(unittest.TestCase):
    def setUp(self):
        warnings.filterwarnings("ignore", category=ResourceWarning,
                                message="unclosed.*<ssl.SSLSocket.*>")

        self.config = Config()
        self.s3 = get_s3_client(self.config)
        self.bucket = self.config.bucket
        self.key = 'temp'

    def tearDown(self):
        self.s3.delete_object(Bucket=self.bucket, Key=self.key)

    def test_fileobj_upload(self):
        """ Test fileobj upload to S3. """
        # Given
        test_string = b'hello world'

        with BytesIO(test_string) as f:
            self.s3.upload_fileobj(f, self.bucket, self.key)

        # When
        with BytesIO() as f:
            self.s3.download_fileobj(self.bucket, self.key, f)
            f.seek(0)
            out = f.read()

        # Then
        self.assertEqual(test_string, out)


if __name__ == '__main__':
    unittest.main()
