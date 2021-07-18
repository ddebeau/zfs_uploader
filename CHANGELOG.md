# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
