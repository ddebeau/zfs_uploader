import subprocess

SUBPROCESS_KWARGS = dict(stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         encoding='utf-8')


class ZFSError(Exception):
    """ Baseclass for ZFS exceptions. """


def list_snapshots():
    """ List snapshots. """
    cmd = ['zfs', 'list', '-p', '-t', 'snapshot']
    out = subprocess.run(cmd, **SUBPROCESS_KWARGS)

    lines = out.stdout.splitlines()
    snapshots = {}

    if lines:
        header = lines[0].split()
        for data in lines[1:]:
            name = data.split()[0]
            snapshots.update(
                {name: {k: v for k, v in zip(header[1:], data.split()[1:])}}
            )

    return snapshots


def create_snapshot(filesystem, snapshot_name):
    """ Create filesystem snapshot. """
    cmd = ['zfs', 'snapshot', f'{filesystem}@{snapshot_name}']
    return subprocess.run(cmd, **SUBPROCESS_KWARGS)


def create_filesystem(filesystem):
    """ Create filesystem. """
    cmd = ['zfs', 'create', filesystem]
    return subprocess.run(cmd, **SUBPROCESS_KWARGS)


def destroy_snapshot(filesystem, snapshot_name):
    """ Destroy filesystem snapshot. """
    cmd = ['zfs', 'destroy', f'{filesystem}@{snapshot_name}']
    return subprocess.run(cmd, **SUBPROCESS_KWARGS)


def destroy_filesystem(filesystem):
    """ Destroy filesystem and filesystem snapshots. """
    cmd = ['zfs', 'destroy', '-r', filesystem]
    return subprocess.run(cmd, **SUBPROCESS_KWARGS)


def get_snapshot_send_size(filesystem, snapshot_name):
    cmd = ['zfs', 'send', '--parsable', '--dryrun',
           f'{filesystem}@{snapshot_name}']
    out = subprocess.run(cmd, **SUBPROCESS_KWARGS)
    return out.stdout.splitlines()[1].split()[1]


def get_snapshot_send_size_inc(filesystem, snapshot_name_1, snapshot_name_2):
    cmd = ['zfs', 'send', '--parsable', '--dryrun', '-i',
           f'{filesystem}@{snapshot_name_1}',
           f'{filesystem}@{snapshot_name_2}']
    out = subprocess.run(cmd, **SUBPROCESS_KWARGS)
    return out.stdout.splitlines()[1].split()[1]


def open_snapshot_stream(filesystem, snapshot_name, mode):
    """ Open snapshot stream. """
    if mode == 'r':
        cmd = ['zfs', 'send', '-w', f'{filesystem}@{snapshot_name}']
        return subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
    elif mode == 'w':
        cmd = ['zfs', 'receive', '-F', f'{filesystem}@{snapshot_name}']
        return subprocess.Popen(cmd, stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
    else:
        raise ValueError('Mode must be r or w')


def open_snapshot_stream_inc(filesystem, snapshot_name_1, snapshot_name_2):
    """ Open incremental snapshot read stream. """
    cmd = ['zfs', 'send', '-i', f'{filesystem}@{snapshot_name_1}',
           f'{filesystem}@{snapshot_name_2}']
    return subprocess.Popen(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
