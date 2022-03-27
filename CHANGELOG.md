# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Fix bug where the snapshot size was incorrect due to the command missing the
  raw mode argument.
[#60](https://github.com/ddebeau/zfs_uploader/issues/60)

## [0.7.2](https://github.com/ddebeau/zfs_uploader/compare/0.7.1...0.7.2) 2022-02-09

### Fixed

- Fix bug where incremental snapshot streams were not being sent in raw mode
[#58](https://github.com/ddebeau/zfs_uploader/pull/58)

## [0.7.1](https://github.com/ddebeau/zfs_uploader/compare/0.7.0...0.7.1) 2022-02-08

### Fixed

- Fix `misfire_grace_time` issue where a job could get skipped if it was 
  scheduled too close to another job.
[#56](https://github.com/ddebeau/zfs_uploader/issues/56)

## [0.7.0](https://github.com/ddebeau/zfs_uploader/compare/0.6.0...0.7.0) 2022-01-27

### Added

- Add support for S3-compatible services.
[#53](https://github.com/ddebeau/zfs_uploader/pull/53)

## [0.6.0](https://github.com/ddebeau/zfs_uploader/compare/0.5.0...0.6.0) 2021-08-29

### Changed

- Add `max_incremental_backups_per_full` config option. Setting the option 
  allows for multiple full backups. Do not set option for one full backup 
  and one or more incremental backups.
[#29](https://github.com/ddebeau/zfs_uploader/issues/29)

- Replace `max_incremental_backups` config option with `max_backups`, to 
  avoid having to set a limit for incremental and full backups. Full 
  backups are only removed when there are no dependent incremental backups.
  
### Fixed

- Remove `misfire_grace_time` so that jobs always run if they are late.
[#49](https://github.com/ddebeau/zfs_uploader/issues/49)

## [0.5.0](https://github.com/ddebeau/zfs_uploader/compare/0.4.2...0.5.0) 2021-08-02

### Added

- Add project documentation to README

- Send all snapshots in raw mode. Encryption is maintained for encrypted 
  filesystems and unencrypted filesystems are sent with compression (if 
  required features are enabled).
[#46](https://github.com/ddebeau/zfs_uploader/issues/46)

## [0.4.2](https://github.com/ddebeau/zfs_uploader/compare/0.4.1...0.4.2) 2021-07-18

### Fixed

- Fix time_elapsed for transfer callback.
[#44](https://github.com/ddebeau/zfs_uploader/issues/44)

## [0.4.1](https://github.com/ddebeau/zfs_uploader/compare/0.4.0...0.4.1) 2021-07-18

### Added

- Add more info to log messages.

## [0.4.0](https://github.com/ddebeau/zfs_uploader/compare/0.3.1...0.4.0) 2021-07-18

### Added

- Add callback for S3 upload and download progress.
[#26](https://github.com/ddebeau/zfs_uploader/issues/26)

### Changed

- Switch to logfmt from custom format. 
[#40](https://github.com/ddebeau/zfs_uploader/issues/40)
  
### Fixed

- Check for configuration file before running CLI commands.
[#36](https://github.com/ddebeau/zfs_uploader/issues/36)
  
- Exit early if required job arguments are missing.
[#24](https://github.com/ddebeau/zfs_uploader/issues/24)

## [0.3.1](https://github.com/ddebeau/zfs_uploader/compare/0.3.0...0.3.1) 2021-07-15
  
### Fixed
  
- Fix bug where pip install returns an error about the script entry point. 
[#35](https://github.com/ddebeau/zfs_uploader/issues/38)

## [0.3.0](https://github.com/ddebeau/zfs_uploader/compare/0.2.0...0.3.0) 2021-07-15

### Added

- Add destination filesystem option to `zfsup restore` 
[#32](https://github.com/ddebeau/zfs_uploader/issues/32)
  
- `zfsup list` now shows all filesystems if one isn't provided. 
[#31](https://github.com/ddebeau/zfs_uploader/issues/31)
 
- `zfsup list` now shows backup size. 
[#28](https://github.com/ddebeau/zfs_uploader/issues/28)
  
### Fixed

- Fix bug where `zfsup restore` doesn't work if the filesystem has changed 
  since the most recent snapshot. The command now destroys snapshots and 
  data that were written after the most recent snapshot. 
[#33](https://github.com/ddebeau/zfs_uploader/issues/33)
  
- Fix bug where only the `zfsup backup` command would log. 
[#35](https://github.com/ddebeau/zfs_uploader/issues/35)
  
## [0.2.0](https://github.com/ddebeau/zfs_uploader/compare/0.1.2...0.2.0) 2021-06-06

### Added

- Add commands for determining snapshot send size

### Changed

- Snapshot properties `used` and `referenced` now return an integer

### Fixed

- Fix bug where the max part number is reached when uploading large snapshots 
[#25](https://github.com/ddebeau/zfs_uploader/issues/25)
- Fix bucket name parameter in sample config file

## [0.1.2](https://github.com/ddebeau/zfs_uploader/compare/0.1.1...0.1.2) 2021-06-05

### Added

- Add changlog

### Fixed

- Fix CLI entrypoint [#22](https://github.com/ddebeau/zfs_uploader/issues/22)

## [0.1.1](https://github.com/ddebeau/zfs_uploader/compare/0.1.0...0.1.1) 2021-05-30

### Fixed

- Fix release automation

## [0.1.0](https://github.com/ddebeau/zfs_uploader/releases/tag/0.1.0) 2021-05-30

### Added

- Initial release
