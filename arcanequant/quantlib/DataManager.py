import pandas as pd
import requests
from datetime import datetime
from pathlib import Path
from typing import Iterable




from arcanequant.quantlib.DataManifestManager import DataManifest

# Placeholder class for file name and package importing
class DataManager():
    """
    DataManager is the class of functions that manages operations related to direct data such as prices, indicators,
    post-processed values etc. (as opposed to DataManifest) which is object oriented and manages the tracking of data
    presence.

    ___________________________________
    Method List:
    > DownloadIntraday - Downloads intraday data of a list (iterable) of given stocks, months and intervals.
    > 
    """




    def DownloadIntraday(path: str, tickers: Iterable[str], intervals: Iterable[int], months: Iterable[str], APIkey: str, saveMode = 'direct', verbose = False): #TODO: ADDED SAVEOPTION, ADD TEXT HERE AND THE CODE TO REFLECT IN SAVEMANIFEST
        """
        This function downloads monthly intraday stock data for a given stock, interval and month.
        The requested data is saved in a directory, and the manifest file in the directory is updated/created.
        The data is directly requested from Alphavantage's API, and requires an API key (free one obtainable).
        Documentation for API here: https://www.alphavantage.co/documentation/
        The primary data file is saved in a .csv format with a naming format "{ticker}_{interval}_{month}.csv".
        The meta data of the file is saved in a .csv format similar to the primary file, with the added suffix '_meta'
        The intraday data is only available for equities listed on US exchanges.
        
        The input is as follows:
        - path - String of the directory into which the intraday data is saved and the data manifest is created or updated.
        - tickers - Iterable of string of typical stock symbols (normally 4 characters) i.e. Microsoft is "MSFT"
        - intervals - Iterable of int of time resolution options 1, 5, 15, 30 and 60 mins.
        - months - Iterable of string of months to request in "YYYY-MM" format, i.e. "2025-01"
        - APIkey - String of the API key required by Alphavantage
        - saveMode - String of the saving direction. Details in notes. 
        - verbose - Bool setting to detail the input, process and/or outputs

        Notes:
        - Upon downloading, the manifest is automatically updated, and saved (as directed).
        - The save options are as follows:
            > 'direct' saves data directly to '.csv' and manifest into '.json' formats
            > 'database' saves data and manifest directly to the given SQL database (engine must be provided)
            > 'both' saves data in both formats
        """
        #TODO: REFACTOR THIS, ADD TYPE HINTING, FIX INPUT TEXT GUIDE AND ADD SAVE OPTIONALITY (BUT NEED TO DECIDE WHAT TO DO WITH META DATA FOR SQL)
        dataManifest = DataManifest()
        
        try:
            dataManifest.loadManifest(path = path)
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
            
                            dataDF.rename(columns={'1. open' : 'Open', '2. high' : 'High', '3. low' : 'Low', '4. close' : 'Close', '5. volume' : 'Volume'}, inplace=True)
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
                            dataManifest.saveManifest(savePath = path, echo = verbose) # Run quietly
                        
        print('Current Manifest:')
        print(dataManifest.DF)
        return
    

##################################
##################################
# Auxiliary code
class APIError(Exception): pass # Error for a API error response (invalid call or API call limit)
##################################
##################################