##### CHANGELOG

## [0.6.0] - 2026-10-22
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

## [0.5.0] - 2026-02-03
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

## [0.4.0] - 2025-08-30
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






