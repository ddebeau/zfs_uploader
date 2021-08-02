# ZFS Uploader
Work in progress. Use at your own risk!

ZFS Uploader is a simple program for backing up ZFS full and incremental 
snapshots to Amazon S3. It supports CRON based scheduling and pruning of 
incremental snapshots.

### Features
- Backup/restore ZFS file systems
- Create incremental and full backups
- Prune incremental backups
- Prune snapshots
- Use any S3 storage class type

### Requirements
- Python 3.6 or higher
- ZFS 0.8.1 or higher (untested on earlier versions)

## Install Instructions
Commands should be run as root.

1. Create a directory and virtual environment
```bash
mkdir /etc/zfs_uploader
cd /etc/zfs_uploader
virtualenv --python python3 env
```

2. Install ZFS Uploader
```bash
source env/bin/activate
pip install zfs_uploader
ln -sf /etc/zfs_uploader/env/bin/zfsup /usr/local/sbin/zfsup
```

3. Write configuration file

See [sample_config.cfg](sample_config.cfg) for config format.
```bash
vi config.cfg
chmod 600 config.cfg
```

4. Start service
```bash
cp zfs_uploader.service /etc/systemd/system/zfs_uploader.service
sudo systemctl enable --now zfs_uploader
```

## Miscellaneous
### Storage class codes
- STANDARD
- REDUCED_REDUNDANCY
- STANDARD_IA
- ONEZONE_IA
- INTELLIGENT_TIERING
- GLACIER
- DEEP_ARCHIVE
- OUTPOSTS

## Release Instructions
1. Increment version in `__init__.py` file

2. Update `CHANGELOG.md` with new version

3. Tag release in GitHub when ready. Add changelog items to release 
   description. GitHub Action workflow will automatically build and push 
   the release to PyPi.
