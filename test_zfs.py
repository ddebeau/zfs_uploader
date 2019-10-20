import unittest
from zfs import (create_filesystem, create_snapshot, destroy_filesystem,
                 open_snapshot_stream, list_snapshots)


class ZFSTests(unittest.TestCase):
    def setUp(self):
        # Given
        self.filesystem = 'hgstPool/test'
        self.snapshot_name = 'snap'
        self.test_file = f'/{self.filesystem}/test_file'

        out = create_filesystem(self.filesystem)
        self.assertEqual(0, out.returncode, msg=out.stderr)

        with open(self.test_file, 'w') as f:
            f.write('hello world')

    def tearDown(self):
        out = destroy_filesystem(self.filesystem)
        self.assertEqual(0, out.returncode, msg=out.stderr)

    def test_create_snapshot(self):
        # When
        out = create_snapshot(self.filesystem, self.snapshot_name)
        self.assertEqual(0, out.returncode, msg=out.stderr)

        # Then
        out = list_snapshots()
        self.assertIn(f'{self.filesystem}@{self.snapshot_name}',
                      list(out.keys()))

    def test_create_send_stream(self):
        # Given
        out = create_snapshot(self.filesystem, self.snapshot_name)
        self.assertEqual(0, out.returncode, msg=out.stderr)

        # When
        with open_snapshot_stream(self.filesystem, self.snapshot_name,
                                  'r') as f:
            snapshot = f.stdout.read()
            stderr = f.stderr.read().decode('utf-8')
        self.assertEqual(0, f.returncode, msg=stderr)

        out = destroy_filesystem(self.filesystem)
        self.assertEqual(0, out.returncode, msg=out.stderr)

        # Then
        with open_snapshot_stream(self.filesystem, self.snapshot_name,
                                  'w') as f:
            f.stdin.write(snapshot)
            stderr = f.stderr.read().decode('utf-8')
        self.assertEqual(0, f.returncode, msg=stderr)

        with open(self.test_file, 'r') as f:
            out = f.read()

        self.assertEqual('hello world', out)
