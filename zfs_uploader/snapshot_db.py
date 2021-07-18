from time import sleep

from zfs_uploader.utils import get_date_time
from zfs_uploader import zfs


class SnapshotDB:
    @property
    def filesystem(self):
        """ ZFS file system. """
        return self._filesystem

    def __init__(self, filesystem):
        """ Create SnapshotDB object.

        Snapshot DB is used for storing Snapshot objects. Creating a
        snapshot will create an actual ZFS snapshot.

        Parameters
        ----------
        filesystem : str
            ZFS filesystem.

        """
        self._filesystem = filesystem
        self._snapshots = {}

        self.refresh()

    def create_snapshot(self):
        """ Create Snapshot object and ZFS snapshot.

        Returns
        -------
        Snapshot

        """
        name = get_date_time()

        if name in self._snapshots:
            # sleep for one second in order to increment name
            sleep(1)
            name = get_date_time()

        out = zfs.create_snapshot(self._filesystem, name)
        if out.returncode:
            raise zfs.ZFSError(out.stderr)

        self.refresh()

        return self._snapshots[name]

    def delete_snapshot(self, name):
        """ Delete Snapshot object and ZFS snapshot.

        Parameters
        ----------
        name : str

        """
        zfs.destroy_snapshot(self._filesystem, name)

        del self._snapshots[name]

    def get_snapshots(self):
        """ Get sorted list of snapshots.

        Most recent snapshot is last.

        Returns
        -------
        list(Snapshot)
            Sorted list of snapshots. Most recent snapshot is last.

        """
        return list(self._snapshots.values())

    def get_snapshot_names(self):
        """ Get sorted list of snapshot names.

        Most recent snapshot name is last.

        Returns
        -------
        list(str)
            Sorted list of snapshot names. Most recent snapshot is last.

        """
        return list(self._snapshots.keys())

    def refresh(self):
        """ Refresh SnapshotDB with latest snapshots. """
        self._snapshots = {}
        for k, v in zfs.list_snapshots().items():
            if self._filesystem in k:
                filesystem, name = k.split('@')
                referenced = int(v['REFER'])
                used = int(v['USED'])

                self._snapshots.update({
                    name: Snapshot(filesystem, name, referenced, used)
                })


class Snapshot:
    """ Snapshot object. """

    @property
    def filesystem(self):
        """ ZFS file system. """
        return self._filesystem

    @property
    def key(self):
        """ filesystem@backup_time identifier """
        return f'{self._filesystem}@{self._name}'

    @property
    def name(self):
        """ Snapshot name. """
        return self._name

    @property
    def referenced(self):
        """ Space referenced by snapshot in bytes. """
        return self._referenced

    @property
    def used(self):
        """ Space used by snapshot in bytes. """
        return self._used

    def __init__(self, filesystem, name, referenced, used):
        """ Create Snapshot object.

        Parameters
        ----------
        filesystem : str
            ZFS filesystem.
        name : str
            Snapshot name.
        referenced : int
            Space referenced by snapshot in bytes.
        used : int
            Space used by snapshot in bytes.

        """
        self._filesystem = filesystem
        self._name = name
        self._referenced = referenced
        self._used = used

    def __eq__(self, other):
        return all((self._filesystem == other._filesystem, # noqa
                    self._name == other._name, # noqa
                    self._referenced == other._referenced, # noqa
                    self._used == other._used # noqa
                    ))

    def __hash__(self):
        return hash((self._filesystem,
                     self._name,
                     self._referenced,
                     self._used
                     ))
