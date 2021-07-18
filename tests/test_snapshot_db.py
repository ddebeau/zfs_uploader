import unittest
import warnings

from zfs_uploader.config import Config
from zfs_uploader.snapshot_db import SnapshotDB
from zfs_uploader.zfs import create_filesystem, destroy_filesystem


class SnapshotDBTests(unittest.TestCase):
    def setUp(self):
        warnings.filterwarnings("ignore", category=ResourceWarning,
                                message="unclosed.*<ssl.SSLSocket.*>")

        config = Config('config.cfg')
        self.job = next(iter(config.jobs.values()))
        self.filesystem = self.job.filesystem

        out = create_filesystem(self.job.filesystem)
        self.assertEqual(0, out.returncode, msg=out.stderr)

    def tearDown(self):
        out = destroy_filesystem(self.job.filesystem)
        if out.returncode:
            self.assertIn('dataset does not exist', out.stderr)

    def test_create_snapshot(self):
        """ Test creating and storing a snapshot. """
        # Given
        snapshot_db = SnapshotDB(self.filesystem)

        # When
        snapshot = snapshot_db.create_snapshot()

        # Then
        self.assertIn(snapshot, snapshot_db.get_snapshots())

        snapshot_db_new = SnapshotDB(self.filesystem)
        self.assertIn(snapshot, snapshot_db_new.get_snapshots())

    def test_create_multiple_snapshots(self):
        """ Test creating mulitple snapshots rapidly. """
        # Given
        snapshot_db = SnapshotDB(self.filesystem)

        # When
        for _ in range(3):
            # Code should pause in order to follow the naming scheme
            snapshot_db.create_snapshot()

        # Then
        self.assertEqual(3, len(snapshot_db.get_snapshots()))

    def test_delete_snapshot(self):
        """ Test removing a snapshot. """
        # Given
        snapshot_db = SnapshotDB(self.filesystem)
        snapshot = snapshot_db.create_snapshot()

        # When
        snapshot_db.delete_snapshot(snapshot.name)

        # Then
        self.assertNotIn(snapshot, snapshot_db.get_snapshots())

        snapshot_db_new = SnapshotDB(self.filesystem)
        self.assertNotIn(snapshot, snapshot_db_new.get_snapshots())
