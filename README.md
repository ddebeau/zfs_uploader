# ZFS Uploader
ZFS Uploader is a simple program for backing up full and incremental ZFS 
snapshots to Amazon S3. It supports CRON based scheduling and can 
automatically remove old snapshots and backups. A helpful CLI (`zfsup`) lets 
you run jobs, restore, and list backups.

### Features
- Backup/restore ZFS file systems
- Create incremental and full backups
- Automatically remove old snapshots and backups
- Use any S3 storage class type
- Helpful CLI

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

Please see the [Configuration File](#configuration-file) section below for 
helpful configuration examples. 
```bash
vi config.cfg
chmod 600 config.cfg
```

4. Start service
```bash
cp zfs_uploader.service /etc/systemd/system/zfs_uploader.service
sudo systemctl enable --now zfs_uploader
```

5. List backups
```bash
zfsup list
```

## Configuration File
The program reads backup job parameters from a configuration file. Default 
parameters may be set which then apply to all backup jobs. Multiple backup 
jobs can be set in one file.

### Parameters
#### bucket_name : str
   S3 bucket name.
#### access_key : str
   S3 access key.
#### secret_key : str
   S3 secret key.
#### filesystem : str
   ZFS filesystem.
#### region : str, default: us-east-1
   S3 region.
#### endpoint : str, optional
   S3 endpoint for alternative services
#### cron : str, optional
   Cron schedule. Example: `* 0 * * *`
#### max_snapshots : int, optional
   Maximum number of snapshots.
#### max_backups : int, optional
   Maximum number of full and incremental backups.
#### max_incremental_backups_per_full : int, optional
   Maximum number of incremental backups per full backup.
#### storage_class : str, default: STANDARD
   S3 storage class.

### Examples
#### Multiple full backups
```ini
[DEFAULT]
bucket_name = BUCKET_NAME
region = us-east-1
access_key = ACCESS_KEY
secret_key = SECRET_KEY
storage_class = STANDARD

[pool/filesystem]
cron = 0 2 * * *
max_snapshots = 7
max_incremental_backups_per_full = 6
max_backups = 7
```

Filesystem is backed up at 02:00 daily. Only the most recent 7 snapshots
are kept. The oldest backup without dependents is removed once there are
more than 7 backups.

#### Backblaze B2 S3-compatible endpoint, full backups
```ini
[DEFAULT]
bucket_name = BUCKET_NAME
region = eu-central-003
access_key = ACCESS_KEY
secret_key = SECRET_KEY
storage_class = STANDARD
endpoint = https://s3.eu-central-003.backblazeb2.com

[pool/filesystem]
cron = 0 2 * * *
max_snapshots = 7
max_incremental_backups_per_full = 6
max_backups = 7
```

##### Structure
full backup (f), incremental backup (i)

1.  f
2.  f i
3.  f i i
4.  f i i i
5.  f i i i i
6.  f i i i i i
7.  f i i i i i i
8.  f i i i i i f
9.  f i i i i f i
10. f i i i f i i
11. f i i f i i i
12. f i f i i i i
13. f f i i i i i
14. f i i i i i i

#### Single full backup
```ini
[DEFAULT]
bucket_name = BUCKET_NAME
region = us-east-1
access_key = ACCESS_KEY
secret_key = SECRET_KEY
storage_class = STANDARD

[pool/filesystem]
cron = 0 2 * * *
max_snapshots = 7
max_backups = 7
```

Filesystem is backed up at 02:00 daily. Only the most recent 7 snapshots
are kept. The oldest incremental backup is removed once there are
more than 7 backups. The full backup is never removed.

##### Structure
full backup (f), incremental backup (i)

1.  f
2.  f i
3.  f i i
4.  f i i i
5.  f i i i i
6.  f i i i i i
7.  f i i i i i i

#### Only full backups
```ini
[DEFAULT]
bucket_name = BUCKET_NAME
region = us-east-1
access_key = ACCESS_KEY
secret_key = SECRET_KEY
storage_class = STANDARD

[pool/filesystem]
cron = 0 2 * * *
max_snapshots = 7
max_incremental_backups_per_full = 0
max_backups = 7
```

Filesystem is backed up at 02:00 daily. Only the most recent 7 snapshots
are kept. The oldest full backup is removed once there are
more than 7 backups. No incremental backups are taken.

##### Structure
full backup (f)

1.  f
2.  f f
3.  f f f
4.  f f f f
5.  f f f f f
6.  f f f f f f
7.  f f f f f f f

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
