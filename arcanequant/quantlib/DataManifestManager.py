from pathlib import Path
from dotenv import load_dotenv, find_dotenv
import os
from io import StringIO
from ast import literal_eval
#from datetime import datetime
import json
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
from typing import Iterable


# CREATE A COMPRESSED MANIFEST (YEARS AND STOCKS ONLY, 2 INDICATES AN INCOMPLETE POINT (I.E. NOT ALL MONTHS OR NOT ALL INTERVALS)

# VALIDATION IS STILL DONE ONLY VIA CSV FILES
# MAYBE CAN EXTEND VALIDATION TO RELY ALSO ON DATABASE
# CAN MAKE VALIDATION BE CHECKED THROUGH ONLY DIRECT FILES, THROUGH DATABASE OR A CROSS-CHECK

# MODIFY DOWNLOADINTRADAY TO ADD SAVE SETTING (SAVE AS SQL OR JSON OPTIONS, AND MAYBE ADD CSV OPTION)

# CONSIDERING TIMEZONE CASTING (FOR LATER) - FUNCTION NAMED CONTEXTUALISE (I.E. USING THE META DATA)?

# CREATE DATAMANAGER FILE/CLASS TO MANAGE DATA SPECIFICALLY (RATHER THAN JUST MANIFEST OR MANIFEST-RELATED STUFF)
# SHOULD CONTAIN DOWNLOAD INTRADAY AND EXTRACT DATA (CAN ALSO HAVE SAVE DATA WHICH POINTS TO SQLSAVE OR SAVECSV)
# MAKE A FUNCTION TO SAVE A FILE TO CSV? (WITH ADDITIONAL PARAMETERS? CURRENTLY NOTHING TO CALL TO SAVE TO CSV)

# ADD CODE TO DIRECTLY API CALL POST-PROCESS DATA (TECH INDICATORS) (ALSO MAKE CODE TO PROCESS IN HOUSE IF DESIRED)
# WILL NEED CODE TO ASSESS ANY STOCKSPLIT INFORMATION AND EITHER MARK FOR RENEW DATA FROM API OR EDIT EXISTING DATA AS NEEDED

# ADD SAVEDATA_SQL - LOADS A FILE FROM CSV AND SAVES INTO SQL.
# MAY NEED TO REMOVE LOADDATA_FROMSQL 

# Placeholder class for file name and package importing
class DataManifestManager():
    """Placeholder class for package-level structure (or possibly also future use)."""
    pass


##################################
##################################
# Data Manifest Class
class DataManifest():
    """
    DataManifests show in a readable format the set of data that exists in the directory the file is in.
    This allows for easier tracking and downloading of missing data as necessary.
    This class also has functions and methods to easily modify values as needed as well as verifying the presence of files.
    The data represented by the manifest can be loaded from its .csv form or from its database (postgreSQL) form.
    The data manifest dataframe itself can be saved or loaded from .json form or from its database (postgreSQL) form.
    The individual data points (indicated in the manifest) can be saved to (SQL) database from here. 
    Note:
    - Consider manifest size and data size (of the range of data you use), SQL is better at working with very large datasets.
    - To save all existing data to SQL (instead of one datapoint), use SQLSync from SQLManager.
    ---
    Value list:
    0 - No file exists
    1 - File exists
    2 - File exists but incomplete (as file covers current time or not updated after month ended)
    Note: 2 is currently marked by you manually, and if validation finds an unmarked file, it assumes it is complete unless stated otherwise.
    ---
    ___________________________________
    Method List:
    > loadManifest - Loads manifest from a .json file into the Data Frame in this class
    > saveManifest - Saves manifest in this class from DataFrame into a .json file 
    > setValue - Sets (or adds) a given value in the manifest
    > validateManifest - Checks the files (or lack thereof) indicated by the manifest
    > reduceManifest - Culls and rows and columns full of zeroes
    > loadData_fromcsv - Loads actual data (of point indicated in manifest) from .csv file
    > loadData_fromsql - Loads actual data (of point indicated in manifest) from database
    > convertDataSQL - Loads actual data (of point indicated in manifest) and saves into SQL database
    """
        
    def __init__(self):
        self.DF = pd.DataFrame(index = pd.MultiIndex(levels = [[],[]], codes = [[],[]], names=['Ticker','Interval']), columns = pd.Index(data = [],name = 'Month'))
         # TODO: CHECK VALIDITY OF SHORTFORM MANIFEST
        self.DFshort = pd.DataFrame(index = pd.Index(data = [],name = 'Ticker'), columns = pd.Index(data = [],name = 'Month'))
        self.directory = None
        self.fileName = 'dataManifest'
        self.SQLengine = None
        
        print('Data Manifest Initialised')

    def __str__(self):
        return f'DataManifest with filename "{self.fileName}" of directory "{self.directory}" and SQL engine {self.SQLengine}.'

    def __repr__(self):
        return f'DataManifest(fileName = {self.fileName}, directory = {self.directory}, SQLengine = {self.SQLengine}). DF dimensions: {self.DF.shape}'

    # Validate Manifest Data (check if file exists, add to manifest or set to 1, else set to 0 or remove)
    def validateManifest(self, fastValidate = True, echo = True):
        """
        This method validates the DataManifest's DataFrame.
        This is done by comparing the stated intraday data file's presence (or lack thereof) in the DataManifest's indicated directory path.
        The files are expected to be named as "{ticker}_{interval}_{month}.csv" where month is YYYY-MM ("2025-01").
        
        Input:
        - fastValidate - Boolean, indicates if fast validation method is to be used.
        - echo - Boolean, indicating verbosity of method.

        Notes:
        - fastValidate works by only considering the entries which are stated as 1 or 2 (exists). This is because the
        ignored entries (set as 0) can be redownloaded later anyways (i.e. when user wants to obtain data for 0 entries).
        """
        print('Validating data manifest DataFrame.')
        
        if fastValidate: print('Conducting fast validation.')
        invalidpoints=[0,0] # Left is invalid, right is total
        # Here we need to separate the stock values, and for each, separate the interval value.
        # For each of these, check if the file exists:
        # If fastValidate, for only the entries which are stated as 1 (exists).
        # This is because ignored files (set to 0) will be redownloaded (or searched if exists) later anyways.
        # Otherwise, for every month entry, and set the value accordingly.
        
        # Unique values of symbols (ticker)
        symbolComponents = list(self.DF.index.get_level_values(0).unique().values)
        # We get these now since it doesn't change across tickers or intervals
        monthComponents = list(self.DF.columns.values)
        
        for symbol in symbolComponents:
            symbolSection = self.DF[(self.DF.index.get_level_values('Ticker') == symbol)]
            # Will try for each interval list in each ticker set (to avoid having to catch errors)
            intervalComponents = list(symbolSection.index.get_level_values(1).unique().values) 
            
            for interval in intervalComponents:
                for month in monthComponents:
                    # Validation operation here:
                    # 0 - No file exists
                    # 1 - File exists
                    # 2 - File exists but incomplete (as file covers current time or not updated after month ended)
                    # Note: 2 is only marked as so elsewhere and assume all files are complete unless stated otherwise
    
                    fileValue = self.DF.loc[((symbol,interval),month)]
                    if not (fileValue == 0 or fileValue == 1 or fileValue == 2):
                        raise ValueError('Manifest has a value that is not 0, 1 or 2. Values are Ticker: ' + symbol + ", Interval: " + str(interval) + ", Month: " + month)
                    
                    if fileValue == 1 or fileValue == 2:
                        # Check if file exists, if so, leave value, otherwise (error), set to 0.
                        invalidpoints[1] += 1 # Adding to no. of tested datapoints
                        try:
                            fileString = r'' + symbol + "_" + str(interval) + "_" + month
                            if echo: print('Searching for file ' + fileString)
                            
                            fileRead = self.loadData_fromcsv(symbol, interval, month, echo = echo)
                        except FileNotFoundError:
                            print('File ' + fileString + ' not found when stated to exist.')
                            self.DF.loc[((symbol,interval),month)] = 0
                            invalidpoints[0] += 1 # Adding to no. of invalid datapoints
                        else:
                            if echo: print('File ' + fileString + ' found.')
                        
                    elif fileValue == 0 and not fastValidate:
                        # Check if file does NOT exist if value zero AND fastValidate is off.
                        # Extra lines but more efficient
                        invalidpoints[1] += 1 # Adding to no. of tested datapoints
                        try:
                            fileString = r'' + symbol + "_" + str(interval) + "_" + month
                            if echo: print('Searching for file ' + fileString)
                            
                            fileRead = self.loadData_fromcsv(symbol, interval, month, echo = echo)
                        except FileNotFoundError:
                            if echo: print('File ' + fileString + ' not found.')
                        else:
                            print('File ' + fileString + ' found when stated to not exist.')
                            self.DF.loc[((symbol,interval),month)] = 1
                            invalidpoints[0] += 1 # Adding to no. of invalid datapoints

        print(f"Number of invalid/tested datapoints in manifest: {invalidpoints[0]}/{invalidpoints[1]}")
        return

    # Method to create and link an SQL connection engine and link to class instance
    def connectSQL(self, dbcred = 'SQLlogin', echo = True):
        """
        Creates connection engine and links to class instance (self.SQLengine) for the database given the requisite details.
        You must have already set up SQL and a database to use this functionality.

        Input:
        - dbcred - String name of the file (not including file extension) containing the connection details.
        Optional Input:
        - echo - Boolean indicating method verbosity

        The file must be an .env file with the following keys:
        DRIVER - the software dealing with the database
        DIALECT - the specific language specification for SQL (i.e. MySQL or PostgreSQL)
        DB_USER - username to access database
        PASSWORD - password linked to username
        HOST_MACHINE - the machine to connect to (containing the database)
        DBNAME - name of the database (must already exist)

        The .env is normally readily creatable/editable if file extensions can be changed manually by the user.

        Example:
        DRIVER=psycopg2:
        DIALECT=postgresql
        DB_USER=myuser
        PASSWORD=mypass
        HOST_MACHINE=localhost
        PORT=5432
        DBNAME=databasename
        """

        env_path = Path(".") / f"{dbcred}.env" # Environment variables file must be same folder as this code
        load_dotenv(dotenv_path=env_path, override=True)
        if not (os.path.isfile(env_path)):
            raise FileNotFoundError(f"The environment file ({env_path}) does not exist")

        driver = os.getenv("DRIVER")
        dialect = os.getenv("DIALECT")
        username = os.getenv("DB_USER") # Not USERNAME as that already exists in OS environment variables and it is not wise to overwrite it
        password = os.getenv("PASSWORD")
        host_machine = os.getenv("HOST_MACHINE")
        port = os.getenv("PORT")
        dbname = os.getenv("DBNAME")
    
        if not all([driver, dialect, username, password, host_machine, port, dbname]):
            raise EnvError('Environment variables are not all provided.')

        connstring = f"{dialect}+{driver}//{username}:{password}@{host_machine}:{port}/{dbname}"
        
                                     #"dialect+driver//username:password@hostname:portnumber/databasename") 
        self.SQLengine = create_engine(connstring)
        if echo: print(f"Connecting to engine: {self.SQLengine}")
        # Note: Code fails if no/wrong database (OperationalError)
        return
    
    # Method to reduce manifest (remove completely 0 rows and columns)
    def reduceManifest(self):
        """
        This method culls any rows and columns full of zeroes.
        """
        print('Culling manifest size')

        # Convert all zeroes to NaNs, and use dropna method, then re-fill with fillna(0)
        self.DF[self.DF == 0] = None

        self.DF.dropna(how='all', inplace=True)
        self.DF.dropna(axis=1, how='all', inplace=True)
        
        self.DF.fillna(0, inplace=True)
        return

    # Method to update manifest (adds columns/index rows as necessary)
    def setValue(self, ticker: str, interval: int, month: str, value: int, sort = True):
        """
        This method updates a value in the DataManifest's DataFrame.
        The method adds columns/indices as necessary.
        """
        if value != 0 and value != 1 and value != 2: raise ValueError('Manifest values must be set to 0, 1 or 2')

        # If any of the symbol, interval (for the symbol) and month values are new, fill all NaNs as 0 in the new rows/cols
        uniqueSymbols = list(self.DF.index.get_level_values(0).unique().values)
        uniqueMonths = list(self.DF.columns.values)
        
        symbolSection = self.DF[(self.DF.index.get_level_values('Ticker') == ticker)]
        uniqueIntervals = list(symbolSection.index.get_level_values(1).unique().values) 

        isnewRowCol = False
        if (ticker not in uniqueSymbols) or (month not in uniqueMonths) or (interval not in uniqueIntervals): isnewRowCol = True 
        
        
        self.DF.loc[((ticker,interval),month)] = int(value)

        if isnewRowCol: self.DF.fillna(int(0), inplace=True)

        if sort: # Sort before updating
            self.DF.sort_values(by=['Ticker','Interval'], inplace=True)
            self.DF.sort_values(by=['Month'], axis=1, inplace=True)
        
        return

    # Method to load .csv market data based on the path of the class, and inputted parameters (ticker, interval, month).
    def loadData_fromcsv(self, ticker: str, interval: int, month: str, convert_DateTime = False, meta = False, echo = True) -> pd.DataFrame | tuple[pd.DataFrame, pd.DataFrame]:
        """
        This method loads a .csv file of stock data, based on the path of the class, and inputted parameters (ticker, interval, month)
        The method assumes the file naming format "{ticker}_{interval}_{month}.csv" where month is YYYY-MM ("2025-01") on the .csv files 
        The method returns the data frame of stock data (and meta data if 'meta' enabled).

        Input:
        - ticker - string of ticker name (i.e. "NVDA")
        - interval - int of interval value (time gap between datapoints)
        - month - string of the year and month of the period (in YYYY-MM format, i.e. "2025-01")

        Optional inputs:
        - convert_DateTime - Boolean indicating to read the DateTime column as datetime64 type (or as a string)
        - meta - Boolean that outputs the relevant meta data as well (in a tuple alongside the actual stock data as two DataFrames)
        - echo - Boolean that provides greater description during execution.
        
        Note:
        > This method only returns a single monthly period, for custom length periods, use ExtractData.
        > If meta data output is enabled, make sure recieve the tuple output (as opposed to a single DataFrame output when meta=False)
        """
        if ( not isinstance(self.directory, str) ) or self.directory == "": raise TypeError('The data manifest directory pointer must be a string pointing to a valid path/folder.')
        
        fileString = r'' + ticker + "_" + str(interval) + "_" + month
        if echo:
            print(rf"Loading data file: {fileString}.csv")
            if meta: print(rf"Loading meta data file: {fileString}_meta.csv")

        fileRead = pd.read_csv(rf"{self.directory}{ticker}/{fileString}.csv")
        metaFileRead = None
        if meta: metaFileRead = pd.read_csv(rf"{self.directory}{ticker}/{fileString}_meta.csv", index_col=0)

        # Convert datetime column from string to datetime64
        if convert_DateTime:
            fileRead['DateTime'] = pd.to_datetime(fileRead['DateTime'], format = "%Y-%m-%d %H:%M:%S")
            if meta: metaFileRead.loc['3. Last Refreshed'] = pd.to_datetime(metaFileRead.loc['3. Last Refreshed'], format = "%Y-%m-%d %H:%M:%S")

        if meta: return (fileRead, metaFileRead)
        else: return fileRead

    # Method to load database market data based on the path of the class, and inputted parameters (ticker, interval, month).
    def loadData_fromsql(self, ticker: str, interval: int, month: str, convert_DateTime = False, postProcess = True, meta = False, echo = True) -> pd.DataFrame:
        """
        This method loads a part of the database of stock data, based on the path of the class, and inputted parameters (ticker, interval, month)
        The method assumes the postgreSQL information from self.SQLengine
        The method returns the data frame of stock data, and calls ExtractData to work.
        Note: This method only returns a single monthly period, for custom length periods, use ExtractData directly.

        Input:
        - ticker - string of ticker name (i.e. "NVDA")
        - interval - int of interval value (time gap between datapoints)
        - month - string of the year and month of the period (in YYYY-MM format, i.e. "2025-01")
        
        Optional inputs:
        - convert_DateTime - Boolean indicating to read the DateTime column as datetime64 type (or as a string)
        - postProcess - Boolean that converts the direct output from SQL into the original form used to save into SQL
        - meta - Boolean that outputs the relevant meta data as well (in a tuple alongside the actual stock data as two DataFrames)
        - echo - Boolean that provides more output during execution.
        """
        from .DataManager import ExtractData
        if echo: print('Extracting data from SQL database')

        sqlDF = ExtractData("stockData", month, month, self, fromSQL = True, postProcess=postProcess, convertDatetime=convert_DateTime, Ticker = ticker, Interval = interval)
        if meta:
            metaSQLDF = ExtractData("metaData", month, month, self, fromSQL = True, postProcess=postProcess, convertDatetime=convert_DateTime, condition=lambda df: (df['2. Symbol'] == ticker) & (df['4. Interval'] == f"{interval}min") )
            return (sqlDF, metaSQLDF)
        else: return sqlDF
    
    # Method to save manifest data into file
    def saveManifest(self, saveTo = 'json', savePath = "", echo = True):
        """
        This method saves the DataFrame in the DataManifest class. Two methods are available:
        One saves into a file with the given directory in a .json format.
        The second saves the manifest into SQL (requires engine).
        Inputs:
        - saveTo - String of the save option. Valid inputs are 'SQL', 'json' or 'both'
        - path - String of the directory to save to (if empty, assumes present self.directory
        as save path,)
        - echo - Bool indicating method verbosity
        Note:
        - If path is empty, assumes present self.directory as save path, but if that is None
        or "", then raises error.
        - If saving to SQL, and no connection is possible, the method will skip SQL save but
        will raise a notice.
        """

        # Guard function against bad path input
        if (not isinstance(savePath,str)) and (saveTo.lower() == 'json' or saveTo.lower() == 'both'):
            raise ValueError('You must provide a valid path to save or load.')

        # Update class-file link details (check for bad inputs first)
        if saveTo.lower() == 'json' or saveTo.lower() == 'both':
            # If savePath empty, set self.directory as savePath, otherwise do other way around
            # If both empty then raise error first
            if savePath == "" or savePath == None:
                if self.directory == None or self.directory == "":
                    raise ValueError('A path was not provided, and this DataManifest does not have a prior saved path. You must provide a path to save to.')
                else:
                    # Update in reverse, obtain from pre-existing self.directory
                    savePath = self.directory
            else:
                # Update as the given savePath
                self.directory = savePath

        if echo: print('Saving Manifest Data')
        
        # Sort before saving
        self.DF.sort_values(by=['Ticker','Interval'], inplace=True)
        self.DF.sort_values(by=['Month'], axis=1, inplace=True)

        # Saving to .json
        if saveTo.lower() == 'both' or saveTo.lower() == 'json':
            filepath = rf"{savePath}{self.fileName}.json"
            if echo: print('Save path/name: ' + filepath)

            manifestJSON = self.DF.to_json()
            
            # Using the with statement to avoid file close errors (though it shouldn't occur) for one-step changes
            with open(filepath,"w") as manifestSave:
                # Indent works if we use json.loads to change into dict as dump indents properly with that
                # Works without indent but is not human-readable
                readableJSON = json.loads(manifestJSON)
                json.dump(readableJSON, manifestSave, indent = 4)
                if echo: print(f"JSON saved successfully to {filepath}")

        # Saving to SQL database
        if saveTo.lower() == 'both' or saveTo.lower() == 'sql':
            # Saving to SQL database (if enabled)
            # Save if the SQLengine is already established, if not try connect to SQL first
            # If run into errors, cancel this operation
            skipEngine = False
            if self.SQLengine is None:
                try:
                    if echo: print('No SQL connection, attempting to create connection engine...')
                    self.connectSQL(echo = echo)
                    if echo: print('Connection engine created')
                except Exception as e:
                    skipEngine = True
                    print(f"An error occurred, skipping connection engine creation: {e}")
                    
            if not skipEngine: # Run if engine already exists or just created 
                from .SQLManager import SQLSave
                SQLSave(self.DF, self.SQLengine, 'manifestTable', ignore_index = True, echo = False)
                if echo: print('Saved to SQL successfully')
        
        return


    # Method to load manifest data and convert into Multi-Index DataFrame
    def loadManifest(self, loadFrom = 'direct', path = "", echo = True):
        """
        This method loads up a manifest file into the DataManifest class' DataFrame attribute.
        Inputs:
        - loadFrom - String indicating where to load from, 'direct' to load from '.json' file, and 'database'
        to load from the SQL database
        - path - String of the directory of the manifestFile (required only for 'direct' load)
        - echo - Boolean indicating method verbosity
        Notes:
        - If the path is "" or None, then the method tries to use the DataManifest's self.directory attribute
        - To load from 'database' the DataManifest must have a valid self.SQLengine connection engine
        """
        if echo: print('Loading Manifest Data')

        if loadFrom not in ('direct','database'): raise ValueError('You must provide a loading method for the manifest.')

        # Database load method
        if loadFrom == 'database':
            if self.SQLengine == None:
                raise ValueError('You must provide a connection engine to use to load the manifest from.')
            
            if echo: print(f'Loading manifest view from engine: {self.SQLengine}')
            
            self.DF = pd.read_sql('SELECT * FROM "manifestData";', self.SQLengine, index_col=['Ticker','Interval'])
            return

        # Direct load method
        if loadFrom == 'direct':
            if not isinstance(path, str):
                raise TypeError('You must provide a valid path to load the manifest from.')
            elif path == "":
                # Try use self.directory (if this is "" or None, raise error)
                if self.directory == None or self.directory == "":
                    input('⚠️ WARNING: No valid load directory detected, returning empty DataManifest. Press Enter to continue, or CTRL+C to abort.')
                    self.DF = pd.DataFrame()
                    return
                else:
                    path = self.directory

            # Update class-file link details
            self.directory = path

            filepath = path + self.fileName + '.json'
            if echo: print('Load path/name: ' + filepath) 
            
            # Here we load the .json file before converting into MIDF
            with open(filepath,) as manifestLoad: # Default is read mode
                # Note: manifestJSON is the string version of loadJSON (dict form), difference is None is null in JSON form
                loadJSON = json.load(manifestLoad)
            
            # After loading json (dict form and None) need to convert to json form (string form and null) to use in pandas
            stringJSON = json.dumps(loadJSON)
            
            # Here we read the JSON form to convert into MIDF and process it to return to original form
            # We don't use json.load as we parse into DF not dict
            self.DF = pd.read_json(StringIO(stringJSON))
            
            # Convert the strings of the 'tuples' in the index into a tuple and put into list to recreate the MultiIndex
            indexlist=[literal_eval(x) for x in self.DF.index]
            
            # Change index from fake tuple into MultiIndex (and rename index axes) 
            self.DF.index = pd.MultiIndex.from_tuples(indexlist,names=['Ticker','Interval'])
            
            # Rename the column axis to Month
            self.DF.rename_axis("Month",axis=1,inplace = True)
        
            # Convert the full datetime month representation to only Year-Month (while keeping it string)
            # Code works if column is both originally string or datetime representation
            if isinstance(self.DF.columns, pd.Index):
                self.DF.columns = pd.to_datetime(self.DF.columns,format="%Y-%m")
                self.DF.columns = self.DF.columns.strftime("%Y-%m")
            elif isinstance(self.DF.columns, pd.DatetimeIndex):
                self.DF.columns = self.DF.columns.strftime("%Y-%m")
            
            # Replace any NaNs with zeroes
            self.DF.fillna(0, inplace=True)
        
            # Convert all values to int (1 or 0)
            for column in self.DF.columns:
                self.DF[column] = self.DF[column].astype(int)
            
            return

##################################
##################################

# ADD CODE TO STITCH TOGETHER SPECIFIC DATA PARTS FROM DIFFERENT SETS FOR ANALYSIS (CORRELATION ETC.)
# ADD CODE TO DIRECTLY API CALL POST-PROCESS DATA (TECH INDICATORS) (ALSO MAKE CODE TO PROCESS IN HOUSE IF DESIRED)
# WILL NEED CODE TO ASSESS ANY STOCKSPLIT INFORMATION AND EITHER MARK FOR RENEW DATA FROM API OR EDIT EXISTING DATA AS NEEDED

##################################
##################################
# Auxiliary code
class EnvError(Exception): pass # Error for missing environment variables
##################################
##################################

