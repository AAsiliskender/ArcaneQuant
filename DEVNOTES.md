################# DEVNOTES #################
##### STRATEGIC TO DO LIST:
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

-

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

### THIS GIT COMMIT:
- Added DataAnalysis.ipynb as an example of conducting data analysis for trading purposes. Trading is not conducted here but some strategies/information is gleaned from the data. Note currently only a small part of ArcaneQuant library is used for this but this will be expanded later as I gain confidence in practical modelling.
- Added method repairManifest, repairs manifest by removing duplicate columns and conducting full validation. Other repair actions may be added later.
- Updated SQLtoDFFormat() function (and therefore the postProcess argument) to consider removing the Interval and Ticker
columns from ExtractData() dataframe output if each has a unique value (i.e. only one Ticker), separately, rather than removing
both if only both are unique.
- Functionally completed following UI elements:
    > DataManifestController
    > SQLController
    > DataController
- Added UI elements showing:
    > If currently, directly-loaded and database-loaded DataManifests are synced.
    > Which DataManifest is linked (to the current manifest being shown in the app).
    > If a selected unit data identifier (ticker, interval, month-year) exists with the specified load option(s).
    > Colour theming for button operations:
        * Basic for normal operations.
        * Green/white for session initialisation operations.
        * Red/white for unsafe or potentially unsafe operations.
        * Yellow/red for highly data-dangerous operations.
        * Black/white for CLI-style code execution operations.
    > Popups for normal operation confirmations, and destructive popups for confirming data-dangerous operations (and a separate theme for extremely dangerous operations like SQLWipe).
- Created new module ModelManager to help manage data models by creating a new dataclass (a first for me) ModelEngine.
- ModelEngine consists of attributes:
    > data - Containing the data pool to be used for the model(s).
    > selection - Containing the name label(s) of the subset to be selected from the data.
    > ops - Anonymous lambda operations (value) available in the engine instance.
    > processDict - A dictionary of named 'processes' (key) that reference a tuple (value) of an operation (ops) and its corresponding
    selection label(s).
    > pipelineDict - A dictionary of named (key) pipelineDict referencing an iterable (value) of tuples containing the names
    of operations to be done in succession. The operations must be ordered correctly and the input/output variable count must be correct.
    > modelsDict - A dictionary containing the name (key) and output (value) of processDict elements after execution.
- ModelEngine methods are:
    > inherit_from - Inherits one, some or all of its attributes from the target ModelEngine.
    > merge - Merges two instances of ModelEngine, prioritising attributes from primary source (a classmethod).
    > execute_process - Executes a specific item in processDict (i.e. one process) by running the relevant pipeline on data
    filtered using ModelEngine.selection (can store output into ModelEngine.models).
    > execute_pipeline - Executes specified pipeline's processes in order, stores output into ModelEngine.models.
    > execute_all_pipe - Executes all pipelines in pipelineDict.
    > models - Outputs the models from self.modelsDict in pandas.DataFrame format.
- Other methods will be added later (relating to saving or loading from long-term storage, or visual settings)


### TO ADD AS GIT COMMIT (FUTURE):


