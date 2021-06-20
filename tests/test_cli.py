import unittest
import warnings
from traceback import format_exception

from click.testing import CliRunner

from zfs_uploader.__main__ import cli
from zfs_uploader.config import Config
from zfs_uploader.zfs import create_filesystem, destroy_filesystem


class CLITests(unittest.TestCase):
    def setUp(self):
        warnings.filterwarnings("ignore", category=ResourceWarning,
                                message="unclosed.*<ssl.SSLSocket.*>")
        self.runner = CliRunner()

        config = Config('config.cfg')
        self.job = next(iter(config.jobs.values()))
        self.bucket = self.job.bucket
        self.test_file = f'/{self.job.filesystem}/test_file'
        self.test_data = str(list(range(100_000)))

        out = create_filesystem(self.job.filesystem)
        self.assertEqual(0, out.returncode, msg=out.stderr)

        with open(self.test_file, 'w') as f:
            f.write(self.test_data)

    def tearDown(self):
        out = destroy_filesystem(self.job.filesystem)
        if out.returncode:
            self.assertIn('dataset does not exist', out.stderr)

        for item in self.bucket.objects.all():
            item.delete()

    def test_list_backups(self):
        """ Test list command. """
        # Given
        self.job.start()
        self.job.start()

        # When
        result = self.runner.invoke(cli, ['list', self.job.filesystem])

        # Then
        self.assertEqual(result.exit_code, 0, msg=_format_exception(result))

    def test_restore_command(self):
        """ Test restore command. """
        # Given
        self.job.start()

        with open(self.test_file, 'a') as f:
            f.write('append')
        self.job.start()

        out = destroy_filesystem(self.job.filesystem)
        self.assertEqual(0, out.returncode, msg=out.stderr)

        # When
        result = self.runner.invoke(cli, ['restore', self.job.filesystem])
        self.assertEqual(result.exit_code, 0, msg=_format_exception(result))

        # Then
        with open(self.test_file, 'r') as f:
            out = f.read()
        self.assertEqual(self.test_data + 'append', out)


def _format_exception(result):
    return ''.join(format_exception(*result.exc_info))
