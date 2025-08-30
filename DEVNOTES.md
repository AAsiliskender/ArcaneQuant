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
- Expose data being used (tabs/tables etc.)
- Complete DataManifestController:
    > Save and validate buttons
    > Save as operation vs save (if save location already exists)

- Complete DataController:
    > Get way to modify listview
    > Get way to make lists in an out of focus
    > Sort ascending or descending on header click
- Popup (for warnings etc.)
- Make a way to close tabs/remove data from tabsModel
- Show graphs
- other stuff (so much to do!)

- Have a way to store existing processed data/analysis and load (dataframes)

- Create a check for equality of DataManifests

- DownloadIntraday must be given saving modalities, and the SQL save option must be coded in/working properly.
- Make code to identify if files in validateManifest() should be numbered as 1 or 2 in manifest (identify creation/modify date and check if it is the month of the data, if so = 2)
- Create SQLLoad (sync from direct data files to database - reverse of SQLSync)

- Update the separate files (in arcanequant) with code in jupy (as it is updated/cleaned/fixed)
- Move ExtractData etc. into a file and remove from jupy
- Create Contextualise (use meta-data to fix stuff like date-time)
- Enable jupy to use these files when running other stuff (also increases work speed)

### CURRENT BUGS:
- Using database/sql only to load datamanifest causes bugs with being able to load unit data (green when checking direct load)
- Loading from sql is slow (for some reason)

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
- Created code in Jupyter file that checks data units (a ticker, interval, month) in database also exists in direct files; if missing, saves database data into direct form.
- Created app.py with GUI to allow intuitive use. Currently contains (incomplete functionality):
    > DataManifestController for loading/saving datamanifests
    > DataController for downloading data and extracting data
    > TabManager (to be renamed) for creating new tabs for tables/graphs
    > TabView (to be renamed) for showing actual data
- To be added later:
    > SQLController for SQL based operations (setup, repair etc.)
    > ViewManager (to be renamed) for editing graph
    > QuantController (to be renamed) for analytical ops
    > CLIOperator (to be renamed) for direct CLI usage in-app
- Created backendInterfacer.py to communicate information between QML-Python


### TO ADD AS GIT COMMIT (FUTURE):

