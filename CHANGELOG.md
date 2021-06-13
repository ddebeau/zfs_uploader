# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Add destination filesystem option to `zfsup restore` 
[#32](https://github.com/ddebeau/zfs_uploader/issues/32)
  
### Fixed

- Fix bug where `zfsup restore` doesn't work if the filesystem has changed 
  since the most recent snapshot. The command now destroys snapshots and 
  data that were written after the most recent snapshot.
[#33](https://github.com/ddebeau/zfs_uploader/issues/33)
  
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
