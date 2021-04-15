import os
import unittest

from zfs_uploader.config import Config
from zfs_uploader.zfs import (create_filesystem, create_snapshot,
                              destroy_filesystem, destroy_snapshot,
                              open_snapshot_stream,
                              open_snapshot_stream_inc, list_snapshots)


class ZFSTests(unittest.TestCase):
    def setUp(self):
        # Given
        config = Config('config.cfg')
        job = next(iter(config.jobs.values()))
        self.filesystem = job.filesystem
        self.snapshot_name = 'snap_1'
        self.test_file = f'/{self.filesystem}/test_file'
        self.test_data = str(list(range(100_000)))

        out = create_filesystem(self.filesystem)
        self.assertEqual(0, out.returncode, msg=out.stderr)

        with open(self.test_file, 'w') as f:
            f.write(self.test_data)

    def tearDown(self):
        out = destroy_filesystem(self.filesystem)
        if out.returncode:
            self.assertIn('dataset does not exist', out.stderr)

    def test_create_snapshot(self):
        """ Create snapshot. """
        # When
        out = create_snapshot(self.filesystem, self.snapshot_name)
        self.assertEqual(0, out.returncode, msg=out.stderr)

        # Then
        out = list_snapshots()
        self.assertIn(f'{self.filesystem}@{self.snapshot_name}',
                      list(out.keys()))

    def test_create_incremental_snapshot(self):
        """ Create incremental snapshot. """
        # When
        out = create_snapshot(self.filesystem, self.snapshot_name)
        self.assertEqual(0, out.returncode, msg=out.stderr)

        with open(self.test_file, 'a') as f:
            f.write('append')

        out = create_snapshot(self.filesystem, 'snap_2')
        self.assertEqual(0, out.returncode, msg=out.stderr)

        # Then
        with open_snapshot_stream(self.filesystem, self.snapshot_name,
                                  'r') as f:
            snapshot = f.stdout.read()
            stderr = f.stderr.read().decode('utf-8')
        self.assertEqual(0, f.returncode, msg=stderr)
        self.assertIn(b'1, 2', snapshot)
        self.assertNotIn(b'append', snapshot)

        with open_snapshot_stream_inc(self.filesystem, self.snapshot_name,
                                      'snap_2') as f:
            snapshot = f.stdout.read()
            stderr = f.stderr.read().decode('utf-8')
        self.assertEqual(0, f.returncode, msg=stderr)
        self.assertIn(b'append', snapshot)
        self.assertNotIn(b'1, 2', snapshot)

    def test_restore_filesystem(self):
        """ Restore filesystem from snapshot stream. """
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
        self.assertEqual(self.test_data, out)

    def test_restore_filesystem_with_increment(self):
        """ Restore filesystem from initial and increment snapshot stream. """
        # Given
        out = create_snapshot(self.filesystem, self.snapshot_name)
        self.assertEqual(0, out.returncode, msg=out.stderr)

        with open(self.test_file, 'a') as f:
            f.write('append')

        out = create_snapshot(self.filesystem, 'snap_2')
        self.assertEqual(0, out.returncode, msg=out.stderr)

        # When
        with open_snapshot_stream(self.filesystem, self.snapshot_name,
                                  'r') as f:
            snapshot_initial = f.stdout.read()
            stderr = f.stderr.read().decode('utf-8')
        self.assertEqual(0, f.returncode, msg=stderr)

        with open_snapshot_stream_inc(self.filesystem, self.snapshot_name,
                                      'snap_2') as f:
            snapshot_increment = f.stdout.read()
            stderr = f.stderr.read().decode('utf-8')
        self.assertEqual(0, f.returncode, msg=stderr)

        out = destroy_filesystem(self.filesystem)
        self.assertEqual(0, out.returncode, msg=out.stderr)

        # Then
        with open_snapshot_stream(self.filesystem, self.snapshot_name,
                                  'w') as f:
            f.stdin.write(snapshot_initial)
            stderr = f.stderr.read().decode('utf-8')
        self.assertEqual(0, f.returncode, msg=stderr)

        with open_snapshot_stream(self.filesystem, 'snap_2', 'w') as f:
            f.stdin.write(snapshot_increment)
            stderr = f.stderr.read().decode('utf-8')
        self.assertEqual(0, f.returncode, msg=stderr)

        with open(self.test_file, 'r') as f:
            out = f.read()
        self.assertEqual(self.test_data + 'append', out)

    def test_destroy_filesystem(self):
        """ Destroy filesystem. """
        out = destroy_filesystem(self.filesystem)
        self.assertEqual(0, out.returncode, msg=out.stderr)
        self.assertFalse(os.path.isfile(self.test_file))

    def test_destroy_snapshot(self):
        """ Destroy snapshot. """
        # Given
        out = create_snapshot(self.filesystem, self.snapshot_name)
        self.assertEqual(0, out.returncode, msg=out.stderr)

        out = create_snapshot(self.filesystem, 'snap_2')
        self.assertEqual(0, out.returncode, msg=out.stderr)

        # When
        out = destroy_snapshot(self.filesystem, self.snapshot_name)
        self.assertEqual(0, out.returncode, msg=out.stderr)

        # Then
        out = list_snapshots()
        self.assertNotIn(f'{self.filesystem}@{self.snapshot_name}',
                         list(out.keys()))
        self.assertIn(f'{self.filesystem}@snap_2', list(out.keys()))
