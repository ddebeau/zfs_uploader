import subprocess

SUBPROCESS_KWARGS = dict(stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         encoding='utf-8')


def list_snapshots():
    cmd = ['zfs', 'list', '-t', 'snapshot']
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
    cmd = ['zfs', 'snapshot', f'{filesystem}@{snapshot_name}']
    out = subprocess.run(cmd, **SUBPROCESS_KWARGS)

    return out


def destroy_snapshot(filesystem, snapshot_name):
    cmd = ['zfs', 'destroy', f'{filesystem}@{snapshot_name}']
    out = subprocess.run(cmd, **SUBPROCESS_KWARGS)

    return out
