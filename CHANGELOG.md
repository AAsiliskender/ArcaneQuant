##### CHANGELOG

## [0.3.0] - 2025-08-04
- Cleaned up some code to output manifest, stock and meta data in one of 3 formats; SQL-acceptable, API-outputted (and ArcaneQuant-compatible), and full-data (also ArcaneQuant-compatible)
- Added ExtractData functionality to extract meta data from SQL
- Created SQLNuke function to clear SQL database of all information (use wisely)
- Added SQLNuke use to SQLRepair for a deepRepair option
- Added function SQLtoDFFormat, takes DataFrames and converts to standard format where applicable (if a unit dataset i.e. 1 ticker,interval and month, converts to standard, otherwise keeps critical markers).
- Added DFtoSQLFormat, taking DataFrames from ArcaneQuant direct file data usable format and converting to SQL acceptable formats.
- Added transact option to ExecuteSQL to execute multi-line autocommit queries such as DROP USER/DATABASE, ALTER USER/DATABASE.
- Added functionality to save meta data to SQL (setting up tables/keys, saving from .csv and loading from SQL back into original API format).
- Reorganised relative imports in ./arcanequant and ./arcanequant/quantlib folders.
- Added guard function for inputting empty manifest for ExtractData (when not using SQL).
- Added return condition for using DataManifest.loadManifest() with an empty load path (returns empty pd.DataFrame)
- Added option to use DataManifest directly; or use either of a storage path for DataManifest, or sqlalchemy connection engine as inputs for using DownloadIntraday()
- Added SQL (and both direct and SQL) save optionality for DownloadIntraday()
- Moved ExtractData() from jupyter file to DataManager

## [0.2.2] - 2025-07-27
- Tested signed commit
- Added update automation script

## [0.2.1] - 2025-07-25
- Creating .bumpversion.cfg and streamlining pipeline
- Updated tag from v0.1.2 to 0.2.1 (as should be)

## [0.2.0] - 2025-07-25
- Cleaning up Readme.md and Changelog.md
- Created DataManager class for general data management operations (such as downloading data or extracting data from .csv or from SQL database)
- Added type hinting for most parameters (as applicable)
- Moved DownloadIntraday to DataManager

## [0.1.3] - 2025-07-23
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