################# DEVNOTES #################


##### STRATEGIC TO DO LIST:
- Install linting/create lint reporting system (for transparency)
- Create DataManager class to manage DownloadIntraday, ExtractData, and Contextualise (to create)
- Add REPL and/or GUI for usage
- Create a session class that can call a datamanifest and do other actions (loading data etc.)

#### DECISIONS REQUIRED:
- Decide on adding SaveDataSQL - LOADS A FILE FROM CSV AND SAVES INTO SQL.
- May need to remove loadData_fromsql (and rename _fromcsv), as ExtractData has both  BUT then do we move loadData into DataManager? Do we want to have all these 'Manager' files be so interconnected?

### SHORT-TERM TO DO:
- Create SQLSync
- Create DataManager and move ExtractData etc. into it
- Add from sql or json option for loadmanifest

- Update the separate files (in arcanequant) with code in jupy (as it is updated/cleaned/fixed)
- Move ExtractData etc. into a file and remove from jupy
- Create Contextualise (use meta-data to fix stuff like date-time)
- Enable jupy to use these files when running other stuff (also increases work speed)

## OPTIONAL TO DO:
- Putting (SQL query) test inputs and expected outputs into json/yaml for cleanliness of test files 
- Separating data acquisition in ExtractData into functions in DataManifestManager or SQLManager and call those (or just make subfunctions called in ExtractData)
- Creating compressed DataManifest form

# NOTES:
- Currently when getting data from SQL, the inferred data (nominal price) is ignored, maybe can fix it?
- ExtractData might be inefficient when filtering for months (esp when using 'all'), check this out
- WHAT HAPPENED TO SQLSYNC? NEED TO COMPLETE OR MOVE THE OTHER FUNCTIONS FROM JUPY TO PYTHON
- TEST CODE, ADD INPUT CYCLE FOR ACTION?


# POTENTIAL FIXES NEEDED DOWN THE LINE:
- ExtractData might need to have the non-temporal filters (ticker, interval etc.) be done in SQL to save time

### TO ADD AS GIT COMMIT:
- Created DataManager class for general data management operations (such as downloading data or extracting data from csv or sql)
- Added argument type hints for methods/functions that require single input types.
- Moved more functions from jupyter file to their python code files (i.e. DataManifestManager and SQLManager)
- Added hint to SQLSave indicating market data requires Ticker and Interval data markers