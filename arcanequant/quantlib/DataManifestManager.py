import pandas as pd
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
import os
import requests


# CREATE A COMPRESSED MANIFEST (YEARS AND STOCKS ONLY, 2 INDICATES AN INCOMPLETE POINT (I.E. NOT ALL MONTHS OR NOT ALL INTERVALS)

# Convert scrape Data to a multiindex DF

# USING SQL FOR ETL
# HAVE A METHOD TO ETL THE DATA, AND HAVE LOAD FUNCTION FOR DATABASE AND FOR CSV FILES, VALIDATION IS STILL DONE ONLY VIA CSV FILES
# MAYBE CAN EXTEND VALIDATION TO RELY ALSO ON DATABASE (AND MAYBE HAVE A MANIFEST TABLE IN DATABASE?)
# DATABASE WILL HAVE ALL THE MONTHLY DATA (OF ONE TIME RESOLUTION) IN ONE TABLE FOR EASE WHEN ANALYSING DATA. WHAT WILL THE PRIMARY/FOREIGN KEY BE?
# DATABASE WILL TAKE INTO ACCOUNT TIMEZONE FOR TIMESTAMP DATA
# MAYBE CAN FILTER FOR MONTH/DATES WITHOUT SPLITTING TIMESTAMP INTO COMPONENTS (OTHERWISE MAYBE I CAN FIND A EASY WORKAROUND)
# WILL NEED TO HAVE CODE TO CONVERT DATABASE TABLE TO DATAFRAME FORM SMOOTHLY (I.E. NO CHANGE IN COLUMN NAMES, DATATYPES ETC.)

# NEED TO HAVE SAVE AS SQL OR JSON OPTIONS

# CONSIDERING TIMEZONE CASTING (FOR LATER)

# CREATE DATAMANAGER FILE/CLASS TO MANAGE DATA SPECIFICALLY (RATHER THAN JUST MANIFEST OR MANIFEST-RELATED STUFF)

# Placeholder class for file name and package importing
class DataManifestManager():
    """Placeholder class for package-level structure (or possibly also future use)."""
    pass



##################################
##################################
# Data Manifest Class
class DataManifest():
    """
    Data Manifests show in a readable format the set of data that exists in the directory the file is in.
    This allows for easier downloading of missing data as necessary.
    This class also has functions and methods to easily modify values as needed as well as verifying the presence of files.
    The data represented by the manifest can be loaded from its .csv form or from its database (SQL) form.
    The data manifest dataframe itself can be saved or loaded from .json form or from its database (SQL) form.
    Note: Consider manifest size and data size (of the range of data you use), SQL is better at working with very large datasets.
    ---
    Value list:
    0 - No file exists
    1 - File exists
    2 - File exists but incomplete (as file covers current time or not updated after month ended)
    Note: 2 is currently marked by you manually, and if validation finds an unmarked file, it assumes it is complete unless stated otherwise.
    ---
    _____
    Method List:
    - loadManifest - Loads manifest from a .json file into the Data Frame in this class
    - saveManifest - Saves manifest in this class from DataFrame into a .json file 
    - setValue - Sets (or adds) a given value in the manifest
    - validateManifest - Checks the files (or lack thereof) indicated by the manifest
    - reduceManifest - Culls and rows and columns full of zeroes
    - loadData_fromcsv - Loads actual data (of point indicated in manifest) from .csv file
    - loadData_fromsql - Loads actual data (of point indicated in manifest) from database
    """
        
    def __init__(self):
        self.DF = pd.DataFrame(index = pd.MultiIndex(levels = [[],[]], codes = [[],[]], names=['Stocks','Interval']), columns = pd.Index(data = [],name = 'Month'))
         # TODO: CHECK VALIDITY OF SHORTFORM MANIFEST
        self.DFshort = pd.DataFrame(index = pd.Index(data = [],name = 'Stocks'), columns = pd.Index(data = [],name = 'Month'))
        self.directory = None
        self.fileName = 'dataManifest'
        self.SQLengine = None
        
        print('Data Manifest Initialised')


    # Validate Manifest Data (check if file exists, add to manifest or set to 1, else set to 0 or remove)
    def validateManifest(self, fullValidate = False, echo = True):
        """This method validates the DataManifest's DataFrame.
        This is done by comparing the stated intraday data file's presence (or lack thereof) in the DataManifest's indicated directory path.
        The files are expected to be named as "{ticker}_{interval}_{month}.csv" where month is YYYY-MM ("2025-01")"""
        print('Validating data manifest DataFrame.')
        
        if fullValidate: print('Conducting full validation.')
        invalidpoints=[0,0] # Left is invalid, right is total
        # Here we need to separate the stock values, and for each, separate the interval value.
        # For each of these, check if the file exists:
        # If fullValidate, for every month entry, and set the value accordingly
        # Otherwise, for only the month entries which are stated as 1 (exists).
        # This is because ignored files (set to 0) will be redownloaded anyways.
        
        # Unique values of symbols (ticker)
        symbolComponents = list(self.DF.index.get_level_values(0).unique().values)
        # We get these now since it doesn't change across tickers or intervals
        monthComponents = list(self.DF.columns.values)
        totalmonthCount = len(monthComponents)
        
        for symbol in symbolComponents:
            symbolSection = self.DF[(self.DF.index.get_level_values('Stocks') == symbol)]
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
                        
                    elif fileValue == 0 and fullValidate:
                        # Check if file does NOT exist if value zero AND full validate is on.
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
    def connectSQL(self, dbcred = 'SQLlogin'):
        """Creates connection engine and links to class instance (self.SQLengine) for the database given the requisite details.
        You must have already set up SQL and a database to use this functionality.
        dbcred is the name of the file containing the details.
        The file must be an .env file with the following keys:
        DRIVER - the software dealing with the database
        DIALECT - the specific language specification for SQL (i.e. MySQL or PostgreSQL)
        DB_USER - username to access database
        PASSWORD - password linked to username
        HOST_MACHINE - the machine to connect to (containing the database)
        DBNAME - name of the database (must already exist)

        The .env is normally readily creatable/editable if file extensions can be changed manually.

        Example:
        DRIVER=psycopg2:
        DIALECT=postgresql
        DB_USER=myuser
        PASSWORD=mypass
        HOST_MACHINE=localhost
        PORT=5432
        DBNAME=databasename
        """
        from sqlalchemy import create_engine
        from pathlib import Path

        env_path = Path(".") / f"{dbcred}.env" # Environment variables file must be same folder as this code
        load_dotenv(dotenv_path=env_path, override=True)
        if not (os.path.isfile(env_path)):
            raise FileNotFoundError(f"The environment file ({env_path}) does not exist")

        driver = os.getenv("DRIVER")
        dialect = os.getenv("DIALECT")
        username = os.getenv("DB_USER") # Not USERNAME as that aLready exists in OS environment variables and it is not wise to overwrite it
        password = os.getenv("PASSWORD")
        host_machine = os.getenv("HOST_MACHINE")
        port = os.getenv("PORT")
        dbname = os.getenv("DBNAME")
    
        if not all([driver, dialect, username, password, host_machine, port, dbname]):
            raise EnvError('Environment variables are not all provided.')

        connstring = f"{dialect}+{driver}//{username}:{password}@{host_machine}:{port}/{dbname}"
        
                                     #"dialect+driver//username:password@hostname:portnumber/databasename") 
        self.SQLengine = create_engine(connstring)
        print(f"Connecting to engine: {self.SQLengine}")
        # Note: Code fails if no/wrong database (OperationalError)
        return
    
    # Method to reduce manifest (remove completely 0 rows and columns)
    def reduceManifest(self):
        """This method culls any rows and columns full of zeroes."""
        print('Culling manifest size')

        # Convert all zeroes to NaNs, and use dropna method, then re-fill with fillna(0)
        self.DF[self.DF == 0] = None

        self.DF.dropna(how='all', inplace=True)
        self.DF.dropna(axis=1, how='all', inplace=True)
        
        self.DF.fillna(0, inplace=True)
        return

    # Method to update manifest (adds columns/index rows as necessary)
    def setValue(self, ticker, interval, month, value, sort = True):
        """ This method updates a value in the DataManifest's DataFrame.
        The method adds columns/indices as necessary."""
        if value != 0 and value != 1 and value != 2: raise ValueError('Manifest values must be set to 0, 1 or 2')

        # If any of the symbol, interval (for the symbol) and month values are new, fill all NaNs as 0 in the new rows/cols
        uniqueSymbols = list(self.DF.index.get_level_values(0).unique().values)
        uniqueMonths = list(self.DF.columns.values)
        
        symbolSection = self.DF[(self.DF.index.get_level_values('Stocks') == ticker)]
        uniqueIntervals = list(symbolSection.index.get_level_values(1).unique().values) 

        isnewRowCol = False
        if (ticker not in uniqueSymbols) or (month not in uniqueMonths) or (interval not in uniqueIntervals): isnewRowCol = True 
        
        
        self.DF.loc[((ticker,interval),month)] = int(value)

        if isnewRowCol: self.DF.fillna(int(0), inplace=True)

        if sort: # Sort before updating
            self.DF.sort_values(by=['Stocks','Interval'], inplace=True)
            self.DF.sort_values(by=['Month'], axis=1, inplace=True)
        
        return

    # Method to load .csv market data based on the path of the class, and inputted parameters (ticker, interval, month).
    def loadData_fromcsv(self, ticker, interval, month, echo = True):
        """ This method loads a .csv file of stock data, based on the path of the class, and inputted parameters (ticker, interval, month)
        The method assumes the file naming format "{ticker}_{interval}_{month}.csv" where month is YYYY-MM ("2025-01") on the .csv files 
        The method returns the data frame of stock data.
        """
        if not isinstance(self.directory, str): raise DirectoryError('The data manifest directory pointer must be to a valid path/folder.')
        
        fileString = r'' + ticker + "_" + str(interval) + "_" + month
        if echo: print(rf"Loading file data: {fileString}.csv")
        
        fileRead = pd.read_csv(rf"{self.directory}{ticker}/{fileString}.csv")

        return fileRead

    # Method to load database market data based on the path of the class, and inputted parameters (ticker, interval, month).
    def loadData_fromsql(self, ticker, interval, month, echo = True):
        """ This method loads a part of the database of stock data, based on the path of the class, and inputted parameters (ticker, interval, month)
        The method assumes the postgreSQL information from []
        The method returns the data frame of stock data.
        """
        
        fileRead=0

        return fileRead
    
    # Method to save manifest data into file
    def saveManifest(self, path, save = 'both', echo = True):
        """This method saves the DataFrame in the DataManifest class into a file with the given directory.
        The save format is .json"""
        
        if not isinstance(path, str): raise PathError('You must provide a valid path to save or load.')
        
        if echo: print('Saving Manifest Data')
        

        # Update class-file link details
        self.directory = path
        filepath = rf"{path}{self.fileName}.json"
        if echo: print('Save path/name: ' + filepath)
        
        # Sort before saving
        self.DF.sort_values(by=['Stocks','Interval'], inplace=True)
        self.DF.sort_values(by=['Month'], axis=1, inplace=True)
        
        manifestJSON = self.DF.to_json()
        
        ### Saving is done after we have read the manifest so we don't lose any data
        # Saving to .json file (always)
        
        # Using the with statement to avoid file close errors (though it shouldn't occur) for one-step changes
        with open(filepath,"w") as manifestSave:
            # Indent works if we use json.loads to change into dict as dump indents properly with that
            # Works without indent but is not human-readable
            readableJSON = json.loads(manifestJSON)
            json.dump(readableJSON, manifestSave, indent = 4)
            if echo: print(f"JSON saved successfully to {filepath}")

        # Saving to SQL database (if possible)
        # Save if the SQLengine is already established, if not try connect to SQL first
        # If run into errors, cancel this operation
        if self.SQLengine is None:
            skipEngine = False
            try:
                if echo: print('No SQL connection, attempting to create connection engine...')
                checkManifest.connectSQL()
                if echo: print('Connection engine created')
            except Exception as e:
                skipEngine = True
                print(f"An error occurred, skipping connection engine creation: {e}")
                
        if not skipEngine: # Run if engine already exists or just created 
            self.DF.to_sql('manifest', self.SQLengine, if_exists='replace')
        
        return


    # Method to load manifest data and convert into Multi-Index DataFrame
    def loadManifest(self, path, echo = True):
        """This method loads up a manifest file into the DataManifest class' DataFrame attribute."""
    
        if not isinstance(path, str): raise PathError('You must provide a valid path to save or load.')
        
        if echo: print('Loading Manifest Data')

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
        self.DF.index = pd.MultiIndex.from_tuples(indexlist,names=['Stocks','Interval'])
        
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


##################################
##################################
# Direct Functions (may incorporate them into some other library or class later)
def DownloadIntraday(path, tickers, intervals, months, APIkey, verbose = False):
    """This function downloads monthly intraday stock data for a given stock, interval and month.
    The requested data is saved in a directory, and the manifest file in the directory is updated/created.
    The data is directly requested from Alphavantage's API, and requires an API key (free one obtainable).
    Documentation for API here: https://www.alphavantage.co/documentation/
    The primary data file is saved in a .csv format with a naming format "{ticker}_{interval}_{month}.csv".
    The meta data of the file is saved in a .csv format similar to the primary file, with the added suffix '_meta'
    The intraday data is only available for equities listed on US exchanges.
    
    The input is as follows:
    - path is the directory into which the intraday data is saved and the data manifest is created or updated.
    - manifestFile is the name of the manifest file to be updated/created 
    - tickers are the list of typical stock string (normally 4 characters) i.e. Microsoft is "MSFT"
    - intervals is a list of int with time resolution options 1, 5, 15, 30 and 60 mins.
    - months to request are in a list of strings in "YYYY-MM" format, i.e. "2025-01"
    - verbose is a bool setting to detail the input, process and/or outputs
    """

    dataManifest = DataManifest()
    
    try:
        dataManifest.loadManifest(path)
    except FileNotFoundError:
        pass # Use the empty MIDF to start with if no file
    
    # Here we scrape past intraday stock data
    # For now, not interested in testing current prices as can test it later by making it past :D
    # But need to have a way of retrieving live data to guide my trading decisions

    for month in months:
        # If current month (today) is same as month requested the dataset will always be updated from API
        # If not, then if data is incomplete (value 2 in manifest) or missing (value 0), request, otherwise don't (as we already have it)
        currentMonth = pd.to_datetime(datetime.now(),format="%Y-%m")
        currentMonth = currentMonth.strftime("%Y-%m")
        update = False
            
        for symbol in tickers:
            for interval in intervals:
                
                # Check if dataset exists before making request (to avoid wasting limited daily calls)
                try:
                    # Check data file here (can either try load directly or check manifest)
                    dataVal = dataManifest.DF.loc[((symbol,interval),month)] # KeyError
                except KeyError:
                    print('Data file not indicated in manifest, requesting from API and saving')
                    update = True

                else:
                    # Since we have a value in the manifest (no KeyError), we check what the number is (0, 1, or 2)
                    if dataVal == 0:
                        update = True
                        print('Data file indicated missing in manifest, requesting from API and saving')

                    # Even if dataVal is 1, if we request current month, it should always update (shouldn't be 1 in the first place)
                    elif dataVal == 1 and month != currentMonth:
                        print('Data file existence indicated in manifest, skipping.')
                        update = False
                    else: # Only remaining option is dataVal is 2 and/or requested month is current month
                        print('Data file indicated incomplete, updating from API.')
                        update = True
            
                finally:
                    # Now we decide to request/update the data from the API (or not)
                    if update:
                        # Taking direct data using Alphavantage's API of intraday (and can even do daily values)
                        alphaURL = rf"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval={interval}min&month={month}&outputsize=full&apikey={APIkey} "
                        if verbose: print(alphaURL)
                        
                        # Requesting data from url
                        r = requests.get(alphaURL)
        
                        # Reading data as json format (dictionary)
                        data = r.json()

                        # Raise error if API call limit reached or if another error message:
                        if "Information" in data:
                            errorStr = data["Information"]
                            raise APIError(f"API call limit reached (call: {symbol}, {interval}min, {month}). Statement from API: {errorStr}")
                        elif "Error Message" in data:
                            errorStr = data["Error Message"]
                            raise APIError(f"Invalid API call ({symbol}, {interval}min, {month}), check IPO of ticker. Statement from API: {errorStr}")
                        elif "Note" in data:
                            errorStr = data["Note"]
                            raise APIError(f"Unintended response ({symbol}, {interval}min, {month}). Statement from API: {errorStr}")
                        elif "Meta Data" not in data:
                            raise APIError(f"The API response does not contain data. Check the API output here: {data}")
                        
                        # Converting the dictionary form to DataFrame
                        scrapeDF = pd.DataFrame.from_dict(data, orient='columns')
                        print(scrapeDF)
                        # This DF contains both meta data and actual data, must split them up first
                        datalabel = f'Time Series ({interval}min)' # To get the header of the actual data
                        
                        metaDF = scrapeDF[['Meta Data']].dropna(subset = ['Meta Data'])
                        dataDF = scrapeDF[[datalabel]].dropna(subset = [datalabel])
        
                        # Actual data post processing (renaming columns etc.)
                        dateTimeCol = dataDF.index # Save datetime index to add later as a column
                        dataDF = pd.DataFrame(list(dataDF[datalabel]))
                        dataDF['DateTime'] = dateTimeCol
        
                        dataDF.rename(columns={'1. open' : 'Open', '2. high' : 'High', '3. low' : 'Low', '4. close' : 'Close', '5. volume' : 'Volume'},inplace=True)
                        dataDF = dataDF[['DateTime','Open','High','Low','Close','Volume']] # Reordering columns
        
                        # Show meta and actual data (if show enabled)
                        if verbose:              
                            print('Meta Data:')
                            print(metaDF)
                            print('Actual Data DataFrame')
                            print(dataDF)
                        
                        # Save Meta Data with index (and excepting a possible lack of folder)
                        try:
                            metaDF.to_csv(rf"{path}{symbol}/{symbol}_{interval}_{month}_meta.csv",index=True)
                        except OSError: # If no folder to save into, create it
                            # Specify the directory
                            new_directory_path = Path(rf"{path}{symbol}")
                            # Create the directory
                            try:
                                new_directory_path.mkdir()
                                print(f"New folder '{new_directory_path}' created successfully.")
                            except FileExistsError: # This error should not occur
                                pass
                            except PermissionError:
                                print(f"Permission denied: Unable to create new directory '{new_directory_path}'.")
                            except Exception as e:
                                print(f"An error occurred: {e}")
                            finally: # Try again (even if other errors)
                                metaDF.to_csv(rf"{path}{symbol}/{symbol}_{interval}_{month}_meta.csv",index=True) 
                        
                        # No need to save index numbering with intraday data
                        dataDF.to_csv(rf"{path}{symbol}/{symbol}_{interval}_{month}.csv",index=False)

                        # Decide to put 1 or 2 in manifest (based on if requested month is the present month) 
                        newValue = 1
                        if month == currentMonth: newValue = 2
                            
                        dataManifest.setValue(symbol, interval, month, newValue, sort = False) # No sort to save time
                        dataManifest.saveManifest(path, echo = verbose) # Run quietly
                    
    print('Current Manifest:')
    print(dataManifest.DF)
    return
##################################
##################################

# ADD CODE TO STITCH TOGETHER SPECIFIC DATA PARTS FROM DIFFERENT SETS FOR ANALYSIS (CORRELATION ETC.)
# ADD CODE TO DIRECTLY API CALL POST-PROCESS DATA (TECH INDICATORS) (ALSO MAKE CODE TO PROCESS IN HOUSE IF DESIRED)
# WILL NEED CODE TO ASSESS ANY STOCKSPLIT INFORMATION AND EITHER MARK FOR RENEW DATA FROM API OR EDIT EXISTING DATA AS NEEDED

##################################
##################################
# Auxiliary code
class APIError(Exception): pass # Error for a API error response (invalid call or API call limit)
class EnvError(Exception): pass # Error for missing environment variables
##################################
##################################

