################# DEVNOTES #################
##### STRATEGIC TO DO LIST:
- TIME TO GO BACK TO ACTUAL BACK-END INSTEAD OF INFRASTRUCTURE, FIX IMPORT LINES AND FINISH DATAMANAGER 
- Install linting/create lint reporting system (for transparency)
- Create DataManager class to manage DownloadIntraday, ExtractData, and Contextualise (to create)
- Add REPL and/or GUI for usage
- Create a session class that can call a datamanifest and do other actions (loading data etc.)
- Right now direct and database storages are coupled, need to fully decouple them (so users can use database only if desired from start to end)

#### DECISIONS REQUIRED:


### SHORT-TERM TO DO:
- DownloadIntraday must be given saving modalities, and the SQL save option must be coded in/working properly.
- Make code to identify if files in validateManifest() should be numbered as 1 or 2 in manifest (identify creation/modify date and check if it is the month of the data, if so = 2)
- Create SQLLoad (sync from direct data files to database - reverse of SQLSync)

- Update the separate files (in arcanequant) with code in jupy (as it is updated/cleaned/fixed)
- Move ExtractData etc. into a file and remove from jupy
- Create Contextualise (use meta-data to fix stuff like date-time)
- Enable jupy to use these files when running other stuff (also increases work speed)

## OPTIONAL TO DO:
- Figure out a cross validation for SQLSync or validateManifest, metaTable also acts as a manifest
- Deal with the pip install lines and determine dependencies or installation structure?
- Move loadData_fromcsv and loadData_fromsql to DataManager (as that is related to data directly and not the manifest.)
- Creating an SQLLoad function which syncs the SQL database into direct form (.csv/.json files etc.), to be the reverse of SQLSync (involves calling many queries with time periods based on manifest indication) 
- Putting (SQL query) test inputs and expected outputs into json/yaml for cleanliness of test files
- Separating data acquisition in ExtractData into functions in DataManifestManager or SQLManager and call those (or just make subfunctions called in ExtractData)
- Creating compressed DataManifest form (and related operations)
- Add text/functionality to DFtoSQL and SQLtoDF for compressed manifest (later)
- Moving DataManifestManager, SQLManager and DataManager outside quantlib (maybe because quantlib should only have quantitative codes (i.e. quantitative library)?)
- Creating a function to fully sync both datasets (without prioritising one over other)
- Improving sync by checking if a specific datapoint already synced

# NOTES:
- Currently when getting data from SQL, the inferred data (nominal price) is ignored, maybe can fix it?

# POTENTIAL FIXES NEEDED DOWN THE LINE:
- "UserWarning: Boolean Series key will be reindexed to match DataFrame index" when twice filtering data after ExtractData
- ExtractData might need to have the non-temporal filters (ticker, interval etc.) be done in SQL to save time

### THIS GIT COMMIT:
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

### TO ADD AS GIT COMMIT (FUTURE):

