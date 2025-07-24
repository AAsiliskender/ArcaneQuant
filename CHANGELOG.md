# Changelog

## [0.2.0] - 2025-07-23
- Moved more functions from jupyter file to their python code files (i.e. DataManifestManager and SQLManager)
- Added hint to SQLSave indicating market data requires Ticker and Interval data markers
- Added fetch capability to ExecuteSQL that returns output (using conn.fetchall() from sqlalchemy)
- Added DEVNOTES.md for transparency and to keep a clearer track of things (as opposed to having many TODO in files though I will still use for very short term things.)

## [0.1.2] - 2025-07-13
- Cleanup of some documentation

## [0.1.1] - 2025-05-12
- Implemented version bumping

## [0.1.0] - 2025-05-12
- Added SQL optionality
- Cleanup of existing code

## [0.0.1] - 2025-04-15
### Added
- Initial early release
- Working pandas/.csv file management