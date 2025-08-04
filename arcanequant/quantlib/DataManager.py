import pandas as pd
import requests
from datetime import datetime
from pathlib import Path
from typing import Iterable
import sqlalchemy.engine
from .DataManifestManager import DataManifest

# Placeholder class for file name and package importing
class DataManager():
    """
    DataManager is the class of functions that manages operations related to direct data such as prices, indicators,
    post-processed values etc. (as opposed to DataManifest) which is object oriented and manages the tracking of data
    presence.

    ___________________________________
    Method List:
    > DownloadIntraday - Downloads intraday data of a list (iterable) of given stocks, months and intervals.
    > ExtractData - Extracts an arbitrary given dataset, with given conditions (time range, ticker, etc.) from either database or direct files 
    """
    def __init__(self):
        pass



def DownloadIntraday(tickers: Iterable[str], intervals: Iterable[int], months: Iterable[str], APIkey: str, saveMode = 'direct', dataManifest: DataManifest = None, savePath: str = None, connEngine: sqlalchemy.engine = None, verbose = False):
    """
    This function downloads monthly intraday stock data for a given stock, interval and month.
    The requested data is selected to be either saved in a directory (and the manifest file in the directory is
    updated/created) or saved in a database (the database connection must be provided).
    The data is directly requested from Alphavantage's API, and requires an API key (free one obtainable).
    Documentation for API here: https://www.alphavantage.co/documentation/
    The primary data file is saved in a .csv format with a naming format "{ticker}_{interval}_{month}.csv".
    The meta data of the file is saved in a .csv format similar to the primary file, with the added suffix '_meta'
    The intraday data is only available for equities listed on US exchanges.
    
    The input is as follows:
    - tickers - Iterable of string of typical stock symbols (normally 4 characters) i.e. Microsoft is "MSFT"
    - intervals - Iterable of int of time resolution options 1, 5, 15, 30 and 60 mins.
    - months - Iterable of string of months to request in "YYYY-MM" format, i.e. "2025-01"
    - APIkey - String of the API key required by Alphavantage.
    - saveMode - String of the saving direction. Details in notes.

    Optional input:
    - dataManifest - DataManifest 
    - savePath - String of the directory into which the intraday data is saved and the data manifest is created or updated (if saveMode applicable).
    - connEngine - Connection to SQL database to save into (if saveMode applicable).
    - verbose - Bool setting to detail the input, process and/or outputs

    Notes:
    - The function requires one of the three:
        > dataManifest, if using any saveMode options
        > savePath, if using 'direct' or 'both' saveModes
        > connEngine, if using 'database' or 'both' saveModes
    - If given more than one, dataManifest is prioritised.
    - Upon downloading, the manifest is automatically updated, and saved (as directed).
    - The save options are as follows:
        > 'direct' saves data directly to '.csv' and manifest into '.json' formats
        > 'database' saves data and manifest directly to the given SQL database (engine must be provided)
        > 'both' saves data in both formats
    """
    # Guard function against bad save option input (to avoid calling API and not saving)
    if not saveMode.lower() in ['database', 'direct', 'both']:
        raise ValueError('You must specify a valid save mode - (SQL) database, direct (to .csv file) or both')

    from .DataManifestManager import DataManifest
    from .SQLManager import SQLSave, DFtoSQLFormat

    # Word of warning to user if using 'both' as can only load one DataManifest
    if saveMode.lower() == 'both':
        input("⚠️ Caution: Using the 'both' save setting will overwrite the database DataManifest table with the preferentially loaded direct DataManifest. Press Enter to continue, or CTRL+C to abort.")
    
    ### Loading manifest data, if saveMode 'both' loads directly (.json file)
    # Also sets dataManifest.directory
    if saveMode.lower() in ['direct', 'both']:
        try:
            # Use dataManifest as priority, savePath only if no dataManifest
            if dataManifest == None:
                dataManifest = DataManifest()
                dataManifest.directory = savePath
            elif dataManifest.directory == None or dataManifest.directory == "": # If dataManifest directly provided but empty directory
                dataManifest.directory = savePath

            dataManifest.loadManifest(path = dataManifest.directory, loadFrom = 'direct')
        except FileNotFoundError:
            if saveMode.lower() == 'both':
                input("""⚠️ WARNING: No direct DataManifest file found with 'both' setting used, an empty DataManifest will be created and will overwrite
                      the DataManifest in the database if also not empty... Press Enter to continue, or CTRL+C to abort.""")
            
            pass # Use the empty MIDF to start with if no file

    elif saveMode.lower() == 'database':
        # Use dataManifest as priority, connEngine only if no dataManifest
        if dataManifest == None:
            dataManifest = DataManifest()
            dataManifest.SQLengine = connEngine
        elif dataManifest.directory == None or dataManifest.directory == "": # If dataManifest directly provided but empty connection engine
            dataManifest.SQLengine = connEngine

        dataManifest.loadManifest(loadFrom = 'database')

        # Set dataManifest to be called data manifest from SQL 
        dataManifest.DF = ExtractData(targetData = 'manifest', start = 'all', end = 'all', fromSQL = True) ## CORRECT? IMPORT EXTRACTDATA
        print('dataManifest.DF:') # TODO: COMPLETE
        print(dataManifest.DF)
        
    
    ### Here we scrape past intraday stock data
    ### For now, not interested in testing current prices as can test it later by making it past :D
    ### But need to have a way of retrieving live data to guide my trading decisions
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
        
                        dataDF.rename(columns={'1. open' : 'Open', '2. high' : 'High', '3. low' : 'Low', '4. close' : 'Close', '5. volume' : 'Volume'}, inplace=True)
                        dataDF = dataDF[['DateTime','Open','High','Low','Close','Volume']] # Reordering columns
        
                        # Show meta and actual data (if show enabled)
                        if verbose:              
                            print('Meta Data:')
                            print(metaDF)
                            print('Actual Data DataFrame')
                            print(dataDF)
                        
                        ### Saving data
                        # Save Meta Data with index (and excepting a possible lack of folder)
                        if saveMode.lower() in ['database', 'both']:
                            # Process table (transpose, add month indicator and fix interval value from 'Xmin' to X)
                            metaSQLDF = DFtoSQLFormat(metaDF, dfType = "meta", dataContext = (month,interval))
                            # Save into SQL
                            SQLSave(saveDF = metaSQLDF, engine = dataManifest.SQLengine, saveTable = "metaTable", ignore_index = True, echo = False)
                            pass

                        elif saveMode.lower() in ['direct', 'both']:
                            try:
                                metaDF.to_csv(rf"{savePath}{symbol}/{symbol}_{interval}_{month}_meta.csv",index=True)
                            except OSError: # If no folder to save into, create it
                                # Specify the directory
                                new_directory_path = Path(rf"{savePath}{symbol}")
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
                                    metaDF.to_csv(rf"{savePath}{symbol}/{symbol}_{interval}_{month}_meta.csv",index=True) 
                        
                        # Saving actual stock data
                        # No need to save index numbering with intraday data
                        if saveMode.lower() in ['database', 'both']:
                            # Create dummy dataframe with additional interval and month columns to be used by SQLSave 
                            SQLdataDF = dataDF.copy(deep = True) # Deepcopy to avoid creating additional columns on dataDF
                            SQLdataDF['Ticker'] = symbol
                            SQLdataDF['Interval'] = interval ## ADD CONVERSION FUNCTION HERE
                            SQLSave(saveDF = SQLdataDF, engine = dataManifest.SQLengine, saveTable = "marketTable", ignore_index = True, echo = False)

                        elif saveMode.lower() in ['direct', 'both']:
                            dataDF.to_csv(rf"{savePath}{symbol}/{symbol}_{interval}_{month}.csv",index=False)

                        ### Setting manifest value and saving
                        # Decide to put 1 or 2 in manifest (based on if requested month is the present month) 
                        newValue = 1
                        if month == currentMonth: newValue = 2
                            
                        dataManifest.setValue(symbol, interval, month, newValue, sort = False) # No sort to save time
                        # Save manifest based on saveMode input
                        if saveMode.lower() == 'database':
                            dataManifest.saveManifest(savePath = savePath, saveTo = 'sql', echo = verbose)
                        elif saveMode.lower() == 'direct':
                            dataManifest.saveManifest(savePath = savePath, saveTo = 'json', echo = verbose)
                        elif saveMode.lower() == 'both':
                            dataManifest.saveManifest(savePath = savePath, saveTo = 'both', echo = verbose)
                        
                    
    print('Current Manifest:')
    print(dataManifest.DF)
    return


def ExtractData(targetData: str, start, end, manifest: DataManifest = DataManifest(), postProcess = True, fromSQL = False, convertDatetime = False, condition = None, **filters) -> pd.DataFrame:
    """
    Extracts data from a target dataset (market data or data manifest), with a provided start and end period, using a provided
    extraction method and filtering condition(s) if desired. The function returns a DataFrame. If both boolean mask 'condition' 
    and custom keyword-arguments 'filters' are provided, both are applied.
    Input:
    - targetData - String indicating the targeted dataset is either the market/stock data (marketData), data manifest (dataManifest),
    or compressed data manifest (compDataManifest) containing year-only manifest data.
    - start - (Period, str, datetime, date or pandas.Timestamp) indicating the beginning of the time period to extract, see notes for details.
    - end - (Period, str, datetime, date or pandas.Timestamp) indicating the end of the time period to extract, see notes for details.
    
    Optional inputs:
    - manifest - DataManifest object relating to the data to be extracted (if fromSQL below is False).
    - fromSQL - Boolean indicating where to extract data from, the default (False) takes data directly from .csv for market data and
    .json for manifests, while True takes data from SQL (manifest must have a valid SQL database, and SQLengine attribute).
    - postProcess - Boolean that converts the output into the original form where possible (unit dataset, e.g. stock dataset of one
    ticker in one interval for one month.)
    - convertDatetime - Boolean indicating to convert any Date/Time columns to a datetime64[ns] format. If False, converts to string.
    - condition - A user-custom callable condition for filtering or selecting subset of data, e.g. lambda functions. The
    condition must take in one input (DataFrame) only. Example: lambda df: df['Ticker'] == 'TEST', or for compound filtering,
    use lambda df: (df['2. Symbol'] == 'TEXT') & (df['4. Interval'] == f"{15}min")
    - filters - Custom inputtable keyword-argument (kwarg) variables to act as simple equality filters (i.e. inputting Ticker =
    "TEST" as a kwarg filters the 'Ticker' column for "TEST" datapoints only) 

    Notes:
    - Order of operation: Obtain data (and stitch where necessary with pre-process, e.g. adding Ticker/Month identifiers), apply
    filter/condition/boolean masks, post-process if desired (includes Date/Time conversion).
    - If extracting meta data and filtering or filtering any columns with non-valid Python identifier names (such as '2. Symbol', which
    cannot be used as a variable in Python), use condition instead of **filters custom-keyword arguments.
    - The targetData variable is searches for 'market' or 'stock' to indicate marketData, 'meta' indicating metaData, or 'manifest'
    to search for dataManifest while 'comp' and 'manifest' is both searched for compDatamanifest. 
    - The manifest input DataManifest must have a viable directory, and where applicable, SQLengine, attributes.
    - If manifest data is being extracted directly (fromSQL == False), it is sourced from the .json file (and not the currently
    loaded manifest's DF attribute)
    - Datatypes acceptable to start/end are whatever pandas.Period() accepts, described above (but could change if pandas changes)
    - If any of the start or end inputs contain 'all', the function returns the entire dataset available.
    - If start is None, the function returns all data before end period. If end is None, the function returns all data after start period.
    - The start datetime is the inputted argument, assuming the beginning of any period truncated (i.e. if only date given,
    assumes start of day, or start of year if only year given, etc.)
    - The end datetime is similarly the inputted argument, except it is the end of any period truncated (i.e. EoD if date
    provided or end-of-month if year-month provided)
    - Regarding the start and end periods, for data manifests and compressed data manifests, the period need not be more
    specific than year-month or year, respectively.
    - For market data extraction, for start periods, truncated data assumes the beginning of the period (e.g. if only year
    given, assume year start), while for end periods truncation assumes the end of the period.
    - For data manifests, truncated data is always inclusive of the whole period (e.g. if end period is 2015 for a data
    manifest, then the whole of 2015 is considered), and over-specific data is extended (i.e. if start period is 2015-03-29,
    then the 2015-03 data point is included).
    - If fromSQL variable is not True/Truthy, it is automatically treated as False (come on, you should be able to not ruin
    an optional boolean... :D)
    - Extracting stock data from SQL is faster for larger datasets.
    """
    # Check the start and end inputs for 'all' or None flags
    period_inputs = [start,end]
    if any( (isinstance(period,str) and 'all' in period.lower() ) for period in period_inputs ): # If any 'all' set to max limit
        start = '1900'
        end = '2200-01-01'
    if start is None: 
        start = '1900'
    if end is None: # Doing these two after 'all' check is more efficient (likelier to skip)
        end = '2200-01-01'

    # Get start and end times for start and end args inputted
    startDT = pd.Period(start).start_time
    endDT = pd.Period(end).end_time
    # Get the range of all months that are considered
    startMstr = startDT.strftime("%Y-%m")
    endMstr = endDT.strftime("%Y-%m")

    # List of all months
    monthList = list(pd.date_range(start = startMstr, end = endMstr, freq = 'MS', inclusive = 'both').strftime("%Y-%m").values)

    resultDF = pd.DataFrame()
    
    from .SQLManager import SQLtoDFFormat, ExecuteSQL

    # The method for acquiring market data and manifest data are different, method for filtering for time period is also different for each (SQL vs DataManifest) method
    if fromSQL: # If extracting from SQL
        if 'market' in targetData.lower() or 'stock' in targetData.lower(): # Getting market data
            resultDF = pd.read_sql(f'SELECT * FROM "stockData" WHERE "DateTime" BETWEEN \'{str(startDT)}\' AND \'{str(endDT)}\';', manifest.SQLengine)#, index_col = ['Ticker','Interval'])
            resultDF.drop(columns=['Nominal'], inplace = True) # Drop nominal (for now)

        elif 'comp' in targetData.lower() and 'manifest' in targetData.lower(): # Getting compressed manifest data
            print('To add this functionality in the future...') #TODO: ADD THIS FUNCTIONALITY

        elif 'manifest' in targetData.lower(): # Getting (regular) manifest data
            # Edit view to include new manifest columns
            updateViewQuery = """
                DO $$
                DECLARE
                    col_list text;
                BEGIN
                    SELECT string_agg('m."' || column_name || '"', ', ' ORDER BY ordinal_position)
                    INTO col_list
                    FROM information_schema.columns
                    WHERE table_name = 'manifestTable'
                      AND column_name NOT IN ('TickerID')
                      AND table_schema = 'public';
                
                    EXECUTE format('
                        CREATE OR REPLACE VIEW "manifestData" AS
                        SELECT t."Ticker", %s
                        FROM "manifestTable" m
                        JOIN "tickerTable" t ON m."TickerID" = t."TickerID";
                    ', col_list);
                END $$;
                """
            ExecuteSQL(updateViewQuery, manifest.SQLengine)

            # Extract list of columns that are within the given timeframe (from SQL-side, for efficiency)
            # Dynamically obtain list of columns within given date limits
            colListQuery = f"""
                SELECT string_agg('"' || column_name || '"', ', ')
                FROM information_schema.columns
                WHERE table_name = 'manifestData'
                AND table_schema = 'public'
                AND column_name ~ {"'^\\d{4}-\\d{2}$'"}
                AND to_date(column_name, 'YYYY-MM') BETWEEN '{startDT.strftime("%Y-%m-%d")}' AND '{endDT.strftime("%Y-%m-%d")}';
                """
            
            dynamicCols = ExecuteSQL(colListQuery, manifest.SQLengine, fetch=True)
            dynamicColsString = dynamicCols[0][0]
            fixedColsString = '"Ticker", "Interval"'
            searchString = ""
            if dynamicColsString is not None:
                searchString = f"{fixedColsString}, {dynamicColsString}"
            else:
                searchString = fixedColsString
            
            # Extract whole or partial view 
            resultDF = pd.read_sql(f'SELECT {searchString} FROM "manifestData";', manifest.SQLengine, index_col = ['Ticker','Interval'])

        elif 'meta' in targetData.lower():
            resultDF = pd.read_sql(f'SELECT * FROM "metaData" WHERE "7. Month" BETWEEN \'{monthList[0]}\' AND \'{monthList[-1]}\';', manifest.SQLengine)

        else:
            raise ValueError("The targetData input must match 'market'/'stock', 'manifest', 'meta' or 'comp' & 'manifest'.")

    else: # If extracting data from DataManifest
        # First check months to make sure we remove any months in monthList not within DF columns (otherwise raises error in all non-SQL cases)
        dummyList = monthList.copy()
        for month in monthList:
            if month not in list(manifest.DF.columns.values): dummyList.remove(month)
        monthList = dummyList

        # Load manifest directly from .json file (manifest.DF)
        manifest.loadManifest(path = manifest.directory)
        # Guard function (if empty DataManifest, return empty DataFrame)
        if manifest.DF.empty: 
                print('Empty manifest detected, returning empty DataFrame.')
                return pd.DataFrame() 

        # Getting market data
        if 'market' in targetData.lower() or 'stock' in targetData.lower():
            ### Obtain data directly and stitch together
            # Get combination of ticker/interval to combine with month for .csv file name
            remainderDF = manifest.DF[monthList] # Ignore non-whole months
            remainderDF = remainderDF.loc[(remainderDF != 0).any(axis = 1)] # Gets rid of rows (ticker, interval) that are zeroes (for all months in manifest)
            
            for tick, interv in list(remainderDF.index.values):
                # If data for each month exists in manifest, add full file to aggregate DF
                for month in monthList:
                    if int(manifest.DF.loc[tick, interv][month]): # Check data exists on manifest
                        addDF = manifest.loadData_fromcsv(tick, interv, month, convert_DateTime = convertDatetime, echo = False)
                        # Add ticker name on DF (as normally its not specified, and we are mixing the datasets)
                        addDF['Ticker'] = tick
                        addDF['Interval'] = interv
                        addDF.insert(0, 'Ticker', addDF.pop('Ticker'))
                        addDF.insert(1, 'Interval', addDF.pop('Interval'))

                        resultDF = pd.concat([resultDF, addDF], axis = 0, ignore_index = True)

            resultDF = resultDF[(pd.to_datetime(resultDF.DateTime) >= startDT) & (pd.to_datetime(resultDF.DateTime) <= endDT)]
                
        elif 'comp' in targetData.lower() and 'manifest' in targetData.lower(): # Getting compressed manifest data
            print('To add this functionality in the future...')

        elif 'manifest' in targetData.lower():  # Getting (regular) manifest data        
            # Filter for each month in list (including first and last months)
            resultDF = manifest.DF[monthList]
        
        elif 'meta' in targetData.lower(): # Getting meta data
            ### Obtain data directly and stitch together
            # Get combination of ticker/interval to combine with month for .csv file name
            remainderDF = manifest.DF[monthList] # Ignore non-whole months
            remainderDF = remainderDF.loc[(remainderDF != 0).any(axis = 1)] # Gets rid of rows (ticker, interval) that are zeroes (for all months in manifest)

            for tick, interv in list(remainderDF.index.values): #For each ticker and interval
                # If data for each month exists in manifest, add full file to aggregate DF
                for month in monthList:
                    if int(manifest.DF.loc[tick, interv][month]): # Check data exists on manifest
                        # Get data and concatenate with whole set of extracted
                        _, addDF = manifest.loadData_fromcsv(tick, interv, month, convert_DateTime = convertDatetime, meta = True, echo = False)
                        addDF.loc['7. Month'] = month
                        resultDF = pd.concat([resultDF, addDF], axis = 1, ignore_index = True)

            # If convertDatetime, change to datetime (no actual change), else convert to string. (Use .loc as already in correct row form)
            if convertDatetime:
                resultDF.loc['3. Last Refreshed'] = pd.to_datetime(resultDF.loc['3. Last Refreshed'], format = "%Y-%m-%d %H:%M:%S")
            else:
                resultDF.loc['3. Last Refreshed'] = resultDF.loc['3. Last Refreshed'].astype(str)
        else:
            raise ValueError("The targetData input must match 'market'/'stock', 'manifest', 'meta' or 'comp' & 'manifest'.")
    
    # If custom filter function given and callable, apply it
    if callable(condition):
        resultDF = apply_condition(resultDF, condition)

    # Apply simply equality filters from 'filters' kwargs
    for col, val in filters.items():
        resultDF = resultDF[resultDF[col] == val]

    # Post-process data at the end
    if postProcess:
        if 'market' in targetData.lower() or 'stock' in targetData.lower():
            resultDF = SQLtoDFFormat(resultDF, 'stock', convertDatetime = convertDatetime)
        elif 'meta' in targetData.lower():
            resultDF = SQLtoDFFormat(resultDF, 'meta', convertDatetime = convertDatetime)
        elif 'manifest' in targetData.lower():
            resultDF = SQLtoDFFormat(resultDF, 'manifest', convertDatetime = convertDatetime)
    
    return resultDF


def apply_condition(df, condition) -> pd.DataFrame:
    from pandas.errors import IndexingError
    try:
        # Try row-wise filtering (standard)
        return df[condition(df)]
    except Exception:
        # Fall back to transposed filter (for column filtering based on row values)
        return df.loc[:, condition(df)]


##################################
##################################
# Auxiliary code
class APIError(Exception): pass # Error for a API error response (invalid call or API call limit)
##################################
##################################