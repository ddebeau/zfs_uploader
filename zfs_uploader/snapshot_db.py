from time import sleep

from zfs_uploader.utils import get_date_time
from zfs_uploader import zfs


class SnapshotDB:
    @property
    def file_system(self):
        """ ZFS file system. """
        return self._file_system

    def __init__(self, file_system):
        self._file_system = file_system
        self._snapshots = {}

        self.refresh()

    def create_snapshot(self):
        name = get_date_time()

        if name in self._snapshots:
            # sleep for one second in order to increment name
            sleep(1)
            name = get_date_time()

        out = zfs.create_snapshot(self._file_system, name)
        if out.returncode:
            raise zfs.ZFSError(out.stderr)

        self.refresh()

        return self._snapshots[name]

    def delete_snapshot(self, name):
        zfs.destroy_snapshot(self._file_system, name)

        del self._snapshots[name]

    def get_snapshots(self):
        """ Get sorted list of snapshots.

        Most recent snapshot is last.
        """
        return list(self._snapshots.values())

    def get_snapshot_names(self):
        """ Get sorted list of snapshot names.

        Most recent snapshot name is last.
        """
        return list(self._snapshots.keys())

    def refresh(self):
        """ Refresh SnapshotDB with latest snapshots. """
        self._snapshots = {}
        for k, v in zfs.list_snapshots().items():
            if self._file_system in k:
                file_system, name = k.split('@')
                referenced = v['REFER']
                used = v['USED']

                self._snapshots.update({
                    name: Snapshot(file_system, name, referenced, used)
                })


class Snapshot:
    """ Snapshot object. """

    @property
    def file_system(self):
        """ ZFS file system. """
        return self._file_system

    @property
    def key(self):
        """ file_system@backup_time identifier """
        return f'{self._file_system}@{self._name}'

    @property
    def name(self):
        """ Snapshot name. """
        return self._name

    @property
    def referenced(self):
        """ Space referenced by snapshot. """
        return self._referenced

    @property
    def used(self):
        """ Space used by snapshot. """
        return self._used

    def __init__(self, file_system, name, referenced, used):
        self._file_system = file_system
        self._name = name
        self._referenced = referenced
        self._used = used

    def __eq__(self, other):
        return all((self._file_system == other._file_system,
                    self._name == other._name,
                    self._referenced == other._referenced,
                    self._used == other._used
                    ))

    def __hash__(self):
        return hash((self._file_system,
                     self._name,
                     self._referenced,
                     self._used
                     ))
