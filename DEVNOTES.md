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
- Paste (or type) the commit message, and end with Ctrl+Z and Enter
- After getting rebasing message, press Enter and change 2nd and 3rd commit lines from 'commit' to 'squash' then save and exit by pressing Escape then type :wq! then press Enter

### THIS GIT COMMIT:
- Refactored Python file names to match PEP 8 convention.
- Completely refactored ModelEngine:
    > Now has associated Classes in new files operation.py and resolution.py that simplify works.
    > ModelEngine.assets takes data inputted or calculated, ModelEngine.aliases takes References (new class), ModelEngine.functions take Callables (or RegisteredFunctions, new class), ModelEngine.processes takes Processes (new class), and ModelEngine.pipelines may take (not completed yet) Pipeline classes.
    > Reworked getter and setter methods; forced setting of a variable to a specific attribute, or setting based on type (i.e. Callables go to ModelEngine.functions). New getter methods; get a function from a given name in ModelEngine.functions, or the RegisteredFunction (new Class) from a given name in ModelEngine.functions
    > New methods to execute add and remove a Process from the ModelEngine, methods to 'standardise' (explained later) Processes, and a new way to execute a Process (calls the .execute() in the Process itself).
- New Reference Class; a pointer class that seeks through ModelEngine's attributes/variables to get a variable. Allows for lazy execution and consistent referencing of a dataset. Also allows for key-based pointers to allow switching the key-linked value to rerun a Process (for example) smoothly. Has attributes:
    > A starting pointer 'source' (str)
    > A 'path' direction (ordered Iterable[Any] or None if no pathing)  
    > Resolves for the value step by step following the iterable order. Currently resolves through common built-in data types and pandas.DataFrame and pandas.Series. Also can path through another Reference as a user short-cut.
- New Process Class; container for an operation/function and inputs. Has attributes and methods for:
    > Associated 'name' (str)
    > An 'operation' core to execution (Callable or RegisteredFunction or str in case of referring to ModelEngine.functions)
    > Positional input arguments 'input_args' (Iterable[Any])
    > Keyword input arguments 'input_kwargs' (dict[str, Any])
    > Output name key 'output_dest' (str)
    > Method self.execute(optional ModelEngine and return_output flag); executes the Process (using the engine if given), and returns the output directly (if flagged). Processes any Reference/Deferred inputs to Any/Reference types before execution.
- New Deferred Class; allows stopping resolving Reference when executing Process (if needed for whatever reason). Executed Deferred becomes the Reference contained in Deferred.reference.
- New RegisteredFunction Class; saves the Callable's source information into 'source_info' as a SourceInfo class, and retains functionality by containing the 'function' in self.function. Calling this Class calls the contained function.
- New SourceInfo Class; contains a functions source information. Contains:
    > The 'module_name' of the function. Is 'builtins' if a Python builtin function.
    > The 'source_code' of the function, if applicable (not to builtins or code executed in other languages).
    > The 'source_file' of the function, if applicable.
    > The list of 'source_lines' of the function, if applicable.
    > The 'start_line' of the function within the source file, if applicable.
    > The 'hash_id' of the source code, to distinguish functions/implementations carrying the same function name.
- Modified the __init__ method in DataManifest to allow initialisation of the dataframe, data source reference (directory/SQLengine) at the start (instead of using DataManifest() then dm.loadManifest(...)).
- Added data model/dunder methods to DataManifest class. Corrected the __repr__ method.
- Added Unit and Integration tests for ModelEngine, Reference, Deferred, RegisteredFunction, SourceInfo and Process (final Integration tests not yet complete).

### TO ADD AS GIT COMMIT (FUTURE):


