################# DEVNOTES #################
##### STRATEGIC TO DO LIST:
- Check if/how to use metaclasses
- Create a trading function (for example with kraken REST API).
- TIME TO GO BACK TO ACTUAL BACK-END INSTEAD OF INFRASTRUCTURE, FIX IMPORT LINES AND FINISH DATAMANAGER 
- Install linting/create lint reporting system (for transparency)
- Create DataManager class to manage DownloadIntraday, ExtractData, and Contextualise (to create)
- Add REPL and/or GUI for usage
- Create a session class that can call a datamanifest and do other actions (loading data etc.)
- Right now direct and database storages are coupled, need to fully decouple them (so users can use database only if desired from start to end) [CHECK IF DECOUPLED NOW]
- Adding ability to call from different APIs (and relevant storage infra for different datatypes like daily, options, crypto etc.)

#### DECISIONS REQUIRED:


### SHORT-TERM TO DO:
- Add __call__ to ModelEngine (execute all (or one))
- Graphs
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

- Have a way to store existing processed data/analysis (from models) and load (dataframes)

- Create a check for equality of DataManifests

- DownloadIntraday must be given saving modalities, and the SQL save option must be coded in/working properly.
- Make code to identify if files in validateManifest() should be numbered as 1 or 2 in manifest (identify creation/modify date and check if it is the month of the data, if so = 2)
- Create SQLLoad (sync from direct data files to database - reverse of SQLSync)

- Update the separate files (in arcanequant) with code in jupy (as it is updated/cleaned/fixed)
- Move ExtractData etc. into a file and remove from jupy
- Create Contextualise (use meta-data to fix stuff like date-time)
- Enable jupy to use these files when running other stuff (also increases work speed)

### CURRENT BUGS:
- Bug when using download intraday with partial datetime (i.e. only 2024), results in wrong naming of files, and
duplicates column in dataframe. Made method to fix duplicates in dataframe, but need fix for partial datetime.
- Fix unit data extraction working with/without source manifest or sql engine
- Separate file linking and file loading
- Loading from sql is slow (for some reason)

## OPTIONAL TO DO:
- Refactor classes into dataclasses (require typehinting, can use typing.Any)
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
- Improving sync by checking if a specific datapoint already synced (instead of the entire database)
- Add multi-cast (1-arg lambda function being casted on multiple columns individually) functionality to work with multi-arg lambda functions


# NOTES:
- Currently when getting data from SQL, the inferred data (nominal price) is ignored, maybe can fix it?

# DEV DOCUMENTATION:
- Background, foreground and elevation of buttons with themes are: (Yellow,Red,10), (Red,White,5) and (basic,Black,2) for very dangerous, potentially dangerous and normal button themes. (For consistent UI)
- Get the git key and log in before pushing update


# POTENTIAL FIXES NEEDED DOWN THE LINE:
- "UserWarning: Boolean Series key will be reindexed to match DataFrame index" when twice filtering data after ExtractData
- ExtractData might need to have the non-temporal filters (ticker, interval etc.) be done in SQL to save time

### HOW TO USE COMMITUPDATE.PS1
- Launch and type major (X+1.0.0) or minor (X.Y+1.0) or patch (X.Y.Z+1)
- Paste (or type) the commit message, press Enter (new line), and end with Ctrl+Z and Enter
- Continue (press Enter) after getting 'starting commit squash' message
- Accept (Y) soft reset message
- Final accept to do git push

### THIS GIT COMMIT:
- Rewrote some of the code in commitupdate.ps1:
    > Added redundant checks for commit history and bump2version execution
    > Instead of rebasing, squashes commits by using soft reset, and commits the staged (but otherwise uncommitted) changes

### TO ADD AS GIT COMMIT (FUTURE):


