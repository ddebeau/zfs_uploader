import unittest
from zfs import create_snapshot, destroy_snapshot, list_snapshots


class ZFSTests(unittest.TestCase):
    def setUp(self):
        # Given
        self.filesystem = 'hgstPool/test'
        self.snapshot_name = 'snap'

    def tearDown(self):
        out = destroy_snapshot(self.filesystem, self.snapshot_name)
        self.assertEqual(0, out.returncode, msg=out.stderr)

    def test_create_snapshot(self):
        # When
        out = create_snapshot(self.filesystem, self.snapshot_name)
        self.assertEqual(0, out.returncode, msg=out.stderr)

        # Then
        out = list_snapshots()
        self.assertIn(f'{self.filesystem}@{self.snapshot_name}',
                      list(out.keys()))
