################# DEVNOTES #################
##### STRATEGIC TO DO LIST:
- Install linting/create lint reporting system (for transparency)
- Create DataManager class to manage DownloadIntraday, ExtractData, and Contextualise (to create)
- Add REPL and/or GUI for usage
- Create a session class that can call a datamanifest and do other actions (loading data etc.)
- Right now direct and database storages are coupled, need to fully decouple them (so users can use database only if desired from start to end)

#### DECISIONS REQUIRED:
- Decide on adding SaveDataSQL - LOADS A FILE FROM CSV AND SAVES INTO SQL.
- May need to remove loadData_fromsql (and rename _fromcsv), as ExtractData has both  BUT then do we move loadData into DataManager? Do we want to have all these 'Manager' files be so interconnected?

### SHORT-TERM TO DO:
- Make a small script that can automatically git add, commit, bump version and sign (of course needs parameter editing to have correct commit message)
- DownloadIntraday must be given saving modalities, and the SQL save option must be coded in/working properly.
- SQLSave
- Find a place to save meta data into SQL
- Clean up quantlib __init__.py to allow for easier imports 
- Create SQLSync
- Create DataManager and move ExtractData etc. into it
- Make code to identify if files in validateManifest() should be numbered as 1 or 2 in manifest (identify creation/modify date and check if it is the month of the data, if so = 2)

- Update the separate files (in arcanequant) with code in jupy (as it is updated/cleaned/fixed)
- Move ExtractData etc. into a file and remove from jupy
- Create Contextualise (use meta-data to fix stuff like date-time)
- Enable jupy to use these files when running other stuff (also increases work speed)

## OPTIONAL TO DO:
- Move loadData_fromcsv and loadData_fromsql to DataManager (as that is related to data directly and not the manifest.)
- Creating an SQLLoad function which syncs the SQL database into direct form (.csv/.json files etc.), to be the reverse of SQLSync (involves calling many queries with time periods based on manifest indication)
- Putting (SQL query) test inputs and expected outputs into json/yaml for cleanliness of test files
- Separating data acquisition in ExtractData into functions in DataManifestManager or SQLManager and call those (or just make subfunctions called in ExtractData)
- Creating compressed DataManifest form (and related operations)
- Moving DataManifestManager, SQLManager and DataManager outside quantlib (maybe because quantlib should only have quantitative codes (i.e. quantitative library)?)

# NOTES:
- Currently when getting data from SQL, the inferred data (nominal price) is ignored, maybe can fix it?
- WHAT HAPPENED TO SQLSYNC? NEED TO COMPLETE OR MOVE THE OTHER FUNCTIONS FROM JUPY TO PYTHON
- TEST CODE, ADD INPUT CYCLE FOR ACTION?

# POTENTIAL FIXES NEEDED DOWN THE LINE:
- SQLSave seemed to not fix column count in database (now seems to work with new downloadintraday function)
- ExtractData might need to have the non-temporal filters (ticker, interval etc.) be done in SQL to save time

### THIS GIT COMMIT:


### TO ADD AS GIT COMMIT (FUTURE):

