import pandas as pd
import sqlalchemy

class SQLManager():
    """Placeholder class for package-level structure or future use."""
    pass


# Functions in this file:
# - SQLSetup - Sets up SQL to be used by DataManifest
# - SQLEstablish - Establishes the database by creating necessary tables/columns
# - SQLRepair - Repairs SQL system by remaking all tables and keys, can even resync data
# - SQLSync - Syncs all data in storage to DataManifest
# - SQLClear - Clears all row data in SQL (excluding precomputed tables like date/time)
# - SetKeysQuery - Provide (or execute) query for setting key(s)
# - DropKeysQuery - Provide (or execute) query for dropping key(s)
# - ExecuteSQL - Executes provided SQL query (needs engine)


# Set up database by creating tables/columns and necessary relations/keys
def SQLSetup(connEngine: sqlalchemy.engine, new = True):
    """
    Sets up SQL to be used by DataManifest. Establishes the set of tables with SQLEstablish,
    then sets all the necessary keys, and creates the view to connect .csv and SQL storage forms.
    Input:
    - connEngine - engine used to connect to SQL

    Optional input:
    - new - Boolean indicating if first-time setup, if False, skips establishing relational tables

    Note: Does not sync any data from .csv to SQL.
    Also, if first-time setup (new is True), also sets up testing infrastructure (testUser, testDatabase etc.)
    """

    if new:
        print('Establishing tables and setting all keys to create the relational database...')
        SQLEstablish(connEngine)
        print('Setting up test infrastructure...')
        testSetUpString = '''
                    DROP DATABASE IF EXISTS "testDatabase";
                    DROP USER IF EXISTS "testUser";
                    
                    CREATE DATABASE "testDatabase";
                    CREATE USER "testUser";
                    ALTER USER "testUser" WITH PASSWORD 'testPassword';
                    ALTER DATABASE "testDatabase" OWNER TO "testUser";
                    '''
        ExecuteSQL(testSetUpString, connEngine)
        

    # TickerTable keys
    SetKeysQuery('tickerTable', 'Ticker', 'primary', engine = connEngine)
    SetKeysQuery('tickerTable', 'TickerID', 'unique', engine = connEngine)
    
    # DateTable keys
    SetKeysQuery('dateTable', 'DateID', 'primary', engine = connEngine)
    SetKeysQuery('dateTable', 'Date', 'unique', engine = connEngine)
    SetKeysQuery('dateTable', 'DayOfWeek', 'secondary', engine = connEngine)
    SetKeysQuery('dateTable', 'Day', 'secondary', engine = connEngine)
    SetKeysQuery('dateTable', 'Month', 'secondary', engine = connEngine)
    
    # TimeTable keys
    SetKeysQuery('timeTable', 'TimeID', 'primary', engine = connEngine)
    SetKeysQuery('timeTable', 'Time', 'unique', engine = connEngine)
    SetKeysQuery('timeTable', 'Hour', 'secondary', engine = connEngine)
    SetKeysQuery('timeTable', 'Minute', 'secondary', engine = connEngine)
    SetKeysQuery('timeTable', 'Second', 'secondary', engine = connEngine)
    
    # ManifestTable keys
    SetKeysQuery('manifestTable', ('TickerID','Interval'), 'primary', engine = connEngine)
    SetKeysQuery('manifestTable', 'TickerID', 'foreign', ref = ('tickerTable','TickerID'), engine = connEngine)
    
    # MarketTable keys
    SetKeysQuery('marketTable', ('TickerID','Interval','DateID','TimeID'), 'primary', engine = connEngine)
    SetKeysQuery('marketTable', ('TickerID','DateID','TimeID'), 'foreign', ref = ( ('tickerTable','TickerID'), ('dateTable','DateID'),
                    ('timeTable','TimeID') ), engine = connEngine)
    SetKeysQuery('marketTable', ('TickerID','Interval'), 'foreign', ref = ('manifestTable',['TickerID','Interval']), engine = connEngine)

    # Can set extra indexes here (hour, or year etc.)

    return

# Establish database by creating necessary tables/columns.
def SQLEstablish(engine: sqlalchemy.engine):
    """
    Establishes the database by creating the necessary tables and columns. Does not create any relations 
    or additional columns.
    Input:
    - engine - SQL engine to connect to database
    """
    # First remove all tables directly via SQL (to remove all dependencies as well)
    clearstring = '''
                    DROP TABLE IF EXISTS "tickerTable" CASCADE;
                    DROP TABLE IF EXISTS "marketTable" CASCADE;
                    DROP TABLE IF EXISTS "dateTable" CASCADE;
                    DROP TABLE IF EXISTS "timeTable" CASCADE;
                    DROP TABLE IF EXISTS "manifestTable" CASCADE;
                '''
    ExecuteSQL(clearstring, engine)

    
    ### Creating TickerTable
    # Set columns and index
    tickerTable = pd.DataFrame(columns=['Ticker'])
    tickerTable.rename_axis("TickerID",axis=0,inplace = True)
    
    # Set datatype of columns
    datatypes = {  
            "TickerID": sqltype.SmallInteger(), 
            "Ticker": sqltype.String(10)
        }
    tickerTable.to_sql('tickerTable', engine, if_exists='replace', index=True, dtype = datatypes)
    # Setting TickerID so that it is always not null and serial (so when new tickers added, gives correct ID)
    tickerSetString = """
                        ALTER TABLE "tickerTable"
                        ALTER COLUMN "TickerID" ADD GENERATED BY DEFAULT AS IDENTITY,
                        ALTER COLUMN "TickerID" SET NOT NULL;
                        """
    ExecuteSQL(tickerSetString, engine)

    ### Creating MarketTable
    # Set columns and index
    marketTable = pd.DataFrame(columns=['DateID','TimeID','TickerID','Interval','Open','High','Low','Close','Volume','Nominal'])
    
    # Set datatype of columns
    datatypes = {  
            "DateID": sqltype.Integer(),
            "TimeID": sqltype.Integer(), 
            "TickerID": sqltype.SmallInteger(),
            "Interval": sqltype.SmallInteger(), 
            "Open": sqltype.Float(),
            "High": sqltype.Float(),
            "Low": sqltype.Float(),
            "Close": sqltype.Float(),
            "Volume": sqltype.BigInteger(),
            "Nominal": sqltype.Float()
        }
    marketTable.to_sql('marketTable', engine, if_exists='replace', index=False, dtype = datatypes)


    ### Creating DateTable and TimeTable (in one command using direct SQL queries)
    wallstring = ""

    # DateTable
    wallstring += f'''
                    DROP TABLE IF EXISTS "dateTable" CASCADE;
    
                    CREATE TABLE "dateTable" (
                        "Date" DATE NOT NULL,
                        "DateID" INT GENERATED ALWAYS AS (
                            (extract(year FROM "Date") * 10000) + 
                            (extract(month FROM "Date") * 100) + 
                            extract(day FROM "Date")
                            ) STORED,
                        "Year" INT GENERATED ALWAYS AS ( extract(year FROM "Date") )STORED,
                        "Month" INT GENERATED ALWAYS AS ( extract(month FROM "Date") )STORED,
                        "Day" INT GENERATED ALWAYS AS ( extract(day FROM "Date") )STORED,
                        "DayOfWeek" INT GENERATED ALWAYS AS ( extract(DOW FROM "Date") )STORED
                    );

                    INSERT INTO "dateTable" ("Date")
                    SELECT generate_series('1900-01-01'::DATE, '2200-01-01'::DATE, '1 day');
                    '''

    # TimeTable
    wallstring += f"""
                    DROP TABLE IF EXISTS \"timeTable\" CASCADE;
    
                    CREATE TABLE \"timeTable\" (
                        \"Time\" TIME NOT NULL,
                        \"TimeID\" INT GENERATED ALWAYS AS (
                            (extract(hour FROM \"Time\") * 10000) + 
                            (extract(minute FROM \"Time\") * 100) + 
                            extract(second FROM \"Time\")
                            ) STORED,
                        \"Hour\" INT GENERATED ALWAYS AS ( extract(hour FROM \"Time\") )STORED,
                        \"Minute\" INT GENERATED ALWAYS AS ( extract(minute FROM \"Time\") )STORED,
                        \"Second\" INT GENERATED ALWAYS AS ( extract(second FROM \"Time\") )STORED
                    );

                    INSERT INTO \"timeTable\" (\"Time\")
                    SELECT generate_series('1900-01-01 00:00:00'::TIMESTAMP, '1900-01-01 23:59:59'::TIMESTAMP, '1 second'::INTERVAL)::TIME;
                    """

    ExecuteSQL(wallstring, engine)

    ### Creating ManifestTable
    # Set columns and index
    manifestTable = pd.DataFrame(columns=['TickerID','Interval'])
    manifestTable.rename_axis("Index",axis=0,inplace = True)
    
    # Set datatype of columns
    datatypes = {  
            "TickerID": sqltype.SmallInteger(),
            "Interval": sqltype.SmallInteger()
        }
    manifestTable.to_sql('manifestTable', engine, if_exists='replace', index=False, dtype = datatypes)

    # After tables are created, set up view(s) for extraction from SQL to pandas
    viewsquery = """
        CREATE OR REPLACE VIEW "stockData" AS
        SELECT
            tickt."Ticker", -- from tickerTable
            mt."Interval",  -- from marketTable
        
            -- Construct DateTime from Date + Time
            -- Date must be DATE (from dateTable) and Time must be TIME (from timeTable)
            (dt."Date" + tt."Time") AS "DateTime",
        
            -- Market data
            mt."Open",
            mt."High",
            mt."Low",
            mt."Close",
            mt."Volume",
            mt."Nominal"
        FROM "marketTable" mt
        JOIN "dateTable" dt ON dt."DateID" = mt."DateID"
        JOIN "timeTable" tt ON tt."TimeID" = mt."TimeID"
        JOIN "tickerTable" tickt ON tickt."TickerID" = mt."TickerID";
        

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
    # The second part of this query makes the manifest view (with Ticker instead of TickerID) in SQL with the
    # columns in the manifestTable, here it has no month cols (as its a new manifestTable), but reusing this
    # should add the new columns
    ExecuteSQL(viewsquery, engine)
    
    return

# Repair SQL table/keys/data
def SQLRepair(dataManifest: pd.DataFrame = None, connEngine: sqlalchemy.engine = None, dataRepair = True, echo = False):
    """
    Repairs SQL system by remaking all keys (and tables as needed) on a database, and resyncing data to SQL if possible.
    
    Optional Inputs:
    - dataManifest - DataManifest type, to connect to database (dataManifest should have an existing SQLengine attribute)
    and to re-sync to database (if the optional directory attribute exists).
    - connEngine - Connection to SQL database to repair
    - dataRepair - Boolean indicating if the data also needs to be repaired (by deleting the SQL dataset and re-syncing
    from the .csv dataset)
    - echo - Echo output/actions from function
    
    Note:
    - At least one of dataManifest or connEngine must be provided
    - The database is indicated by the connection engine within dataManifest (dataManifest.SQLengine), or optionally by
    a directly provided engine through connEngine. If both inputs are given (and dataManifest.SQLengine exists),
    dataManifest is preferentially used. If dataManifest has a directory attribute, after repairing the SQL database, the
    data manifest is validated (to make sure there are no missing files), and then used to resync data files to the SQL
    database.
    If issues arise from syncing .csv data directly to SQL (due to existing data in SQL), enable dataRepair to clear (or
    clear manually using SQLDelete).
    """
    import sqlalchemy.exc as sqlexc
    # Selecting engine to use
    if dataManifest.SQLengine:
        connEngine = dataManifest.SQLengine
    elif not connEngine:
        raise ValueError("You must provide a connection engine through dataManifest.SQLengine or connEngine")

    # Set all keys and establish relational database
    print(f'Repairing SQL database using connection engine {connEngine}...')
    try:
        print('Attempting to drop and reset all keys to recreate the relational database without deleting tables')
        SQLSetup(connEngine, new = False)

    except sqlexc.ProgrammingError: # If any table is missing (none should ever be missing), recreate all tables from scratch, then set keys
        if echo: print('Table(s) missing, recreating all tables before repair...')
        SQLSetup(connEngine, new = True)

    # At the end, sync SQL with .csv files (if dataManifest has a directory provided (i.e. a file storage location))
    if dataManifest.directory:
        SQLSync(dataManifest, not dataRepair, echo)

    
    return

# Saves data into SQL in upsert. (Of Manifest data or Market data)
def SQLSave(saveDF: pd.DataFrame, engine: sqlalchemy.engine, saveTable: str, ignore_index = True, echo = False):
    """
    Saves provided dataset into an SQL table (in an upsert - update-insert - format)
    Inputs:
    - saveDF - DataFrame to upsert into table
    - engine - Connection engine to SQL database
    - saveTable - String of target table name to be upserted into

    Optional inputs:
    - ignore_index - Bool indicating to ignore the index of the inputted dataframe
    - echo - output some information during execution

    Converts Ticker column into TickerID and breaks down DateTime into DateID and TimeID (with YY-mm-dd HH:MM:SS format) before
    inserting into SQL tables. Also updates tickerTable in SQL if new ticker is detected, and adds new columns to manifestTable
    if new columns detected. (Can potentially be used to add user-defined tables, not tested though.)
    Note:
    - NoneType (null) values can be added/inserted into rows as long as it does not violate any NOT NULL constraints.
    - If inserting market data, make sure to label the data with columns for 'Ticker' and 'Interval', otherwise the method will
    raise a null constraint violation error.
    - This function searches for indications of saveTable name. If name is close enough to default table names but incorrect,
    it changes it to defaults, those being 'manifestTable'/'compressedManifestTable' (which looks for letters 'comp' and/or
    'manifest') and for 'marketTable' (looking for 'market' or 'stock'). Any other names not fitting into any of these
    descriptions are treated as is, so be weary of capitalisations.
    """

    # Convert name at start (to deal with possible spell errors)
    if "comp" in saveTable.lower() and "manif" in saveTable.lower():
        saveTable = "compressedManifestTable"
    elif "manif" in saveTable.lower():
        saveTable = "manifestTable"
    elif "stock" in saveTable.lower() or "market" in saveTable.lower():
        saveTable = "marketTable"

    # Convert index into cols (as we will use index = False, and need to access index values)
    saveDF = saveDF.reset_index()
    if ignore_index: saveDF.drop(columns = 'index', axis = 1, inplace = True, errors = 'ignore') # Delete the basic index if it exists (if desired)

    # Break down certain columns (if they exist) to be used with relational tables in SQL (tickerTable, dateTable, timeTable etc.)
    # Ticker column
    if 'Ticker' in saveDF.columns:
        # Get tickerTable from SQL, we will convert Ticker/Symbol into TickerID
        tickerTable = pd.read_sql('SELECT "TickerID", "Ticker" FROM "tickerTable";', con = engine)

        # First check if any tickers not in tickerTable (if so add into SQL table, then re-read)
        newTicks = [tick for tick in list(saveDF.Ticker.unique()) if tick not in list(tickerTable['Ticker'])]
        if len(newTicks) > 0:
            addTicks = pd.DataFrame(columns = ['Ticker'], data = newTicks)
            addTicks.to_sql('tickerTable', engine, if_exists='append', index=False, method = postgres_upsert, chunksize = 1000) # Add new tickers to SQL

            tickerTable = pd.read_sql('SELECT "TickerID", "Ticker" FROM "tickerTable";', con = engine) # Re-read updated table

        # Merge tickerID to data frame
        saveDF = saveDF.merge(tickerTable, on = 'Ticker', how = 'left') # Note that the order doesn't technically matter as SQL matches based on column names

        # Drop Ticker/Symbol col after acquiring TickerID
        saveDF.drop(columns = 'Ticker', axis = 1, inplace = True)

    # DateTime column
    if 'DateTime' in saveDF.columns:
        # Break DateTime into Date and Time, then reconstruct DateID and TimeIDs
        saveDF['DateID'] = pd.to_datetime(saveDF.DateTime, format = '%Y-%m-%d %H:%M:%S').dt.date.astype(str).replace('-','', regex=True)
        saveDF['TimeID'] = pd.to_datetime(saveDF.DateTime, format = '%Y-%m-%d %H:%M:%S').dt.time.astype(str).replace(':','', regex=True)        
        saveDF.drop(columns = 'DateTime', axis = 1, inplace = True)

    # Using a different method to save manifest table (as columns are actively added to SQL table)
    if saveTable == 'compressedManifestTable':
        print('To add compressed manifest functionality')
        pass # TODO: ADD IN CODE TO SAVE COMPRESSED MANIFEST (SIMILAR TO STANDARD MANIFEST)

    elif saveTable == 'manifestTable':
        # Read existing manifest in SQL
        manifestSQL = pd.read_sql(f'SELECT * FROM "{saveTable}"', engine)
        excludeCol = list(manifestSQL.columns.values) # Get existing columns in last manifest
        
        # Drop already existing columns from manifest to-be-inserted (to get only new columns that will be inserted)
        newColsDF = saveDF.drop(columns=excludeCol, inplace = False)
        # Extract columns that did not exist in previous manifest (to add to SQL manually)
        newCol = list(newColsDF.columns.values)

        # Create new columns if needed
        queryAddCol = f'''ALTER TABLE "{saveTable}"'''
        for i in range(len(newCol)):
            col = newCol[i]
            queryAddCol += f'''
                ADD COLUMN "{col}" SMALLINT'''
            if i < len(newCol)-1:
                queryAddCol += ','
            else:
                queryAddCol += ';'
                
        if echo:
            print('Adding new columns to manifestTable')
            print(queryAddCol)
        
        if not len(newCol) == 0: # If new columns to add
            ExecuteSQL(queryAddCol, engine)

        # Whenever saving manifest, update the view after to reflect the new cols (not necessary but good practice,
        # as we update when loading too)
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
        ExecuteSQL(updateViewQuery, engine)
    
    if echo: print('Upserting table into SQL...')
    saveDF.to_sql(saveTable, engine, if_exists='append', index = False, method = postgres_upsert, chunksize = 1000000) # Can optimise chunksize if needed
    return

# Sync SQL data from storage to SQL form
def SQLSync(dataManifest, fastSync = False, echo = False):
    """
    Syncs all data in storage indicated by dataManifest directory with SQL database (in upsert mode).
    The data is transferred from .csv to SQL (not the other way around).

    Input:
    - dataManifest - DataManifest type, indicates the DataManifest and relevant data to sync with SQL.

    Optional inputs:
    - fastSync - Boolean indicating if fast syncing is to be used
    - echo - Echo output/actions from function

    Note:
    - fastSync uses fastValidate on validating manifest, and saves the .csv data (in relational form) to the
    SQL database. Not using fastSync conducts a full validation, and also deletes data from SQL before saving
    (to avoid potential clashes and errors).
    """
    print('Syncing SQL with all existing data in storage...')
    
    if echo: print('Verifying existence of files indicated to exist in DataManifest')
    dataManifest.validateManifest(fastValidate = fastSync, echo = echo)

    # If not fast sync, delete all SQL data first before reloading
    if not fastSync:
        if echo: print('Clearing all data before reload')
        SQLClear(dataManifest.SQLengine, echo)

    if echo: print('Syncing data manifest into SQL database')
    SQLSave(dataManifest.DF, dataManifest.SQLengine, 'manifestTable', echo = echo)
    
    if echo: print('Loading each market data file into SQL database')
    # Sync market table based on dataManifest showing if data exists or not
    # Unique values of ticker
    tickerComponents = list(dataManifest.DF.index.get_level_values(0).unique().values)
    # We get these now since it doesn't change across tickers or intervals
    monthComponents = list(dataManifest.DF.columns.values)

    for ticker in tickerComponents:
        tickerSection = dataManifest.DF[(dataManifest.DF.index.get_level_values('Ticker') == ticker)]
        # Will try for each interval list in each ticker set (to avoid having to catch errors)
        intervalComponents = list(tickerSection.index.get_level_values(1).unique().values) 

        for interval in intervalComponents:
            for month in monthComponents:
                # Key:
                # 0 - No file exists
                # 1 - File exists
                # 2 - File exists but incomplete (as file covers current time or not updated after month ended)

                datapointValue = dataManifest.DF.loc[((ticker,interval),month)]
                # If file exists, load into SQL
                if datapointValue == 1 or datapointValue == 2:
                    fileRead = dataManifest.loadData_fromcsv(ticker, interval, month, echo = echo)
                    SQLSave(fileRead, dataManifest.SQLengine, 'marketTable', echo = echo)

    return

# Clear all row data in SQL (excluding precomputed tables like date/time)
def SQLClear(connEngine, echo = False):
    """
    Clears all data rows in SQL (excluding precomputed date/time dimension tables).
    Inputs:
    - connEngine - Connection to SQL database to repair
    - echo - Echo output/actions from function
    """
    if echo: print('Clearing all datasets in SQL') # TEST THIS FUNCTION

    delQuery = '''
    DELETE FROM "manifestTable";
    DELETE FROM "marketTable";
    DELETE FROM "tickerTable";
    '''

    ExecuteSQL(delQuery, connEngine)
    return

# Provide (or execute) query for setting key(s) for a table (after dropping existing one first)
def SetKeysQuery(tableName, keys, kType = 'primary', ref = None, engine = None, echo = False):
    """
    Provides query to set the keys of a table (from a column(s)) in an SQL database. You must specify the sort of key being set
    (default is primary) and the and the key name (column(s)) and additional reference data, if applicable. Uses postgreSQL dialect.
    You must have an existing valid SQL engine (i.e. self.SQLengine) connection for this to function.
    This method outputs SQL queries (string) to create the key; if the same already exists, the key is dropped and a new one is made.
    
    Input is as follows:
    - tableName - String of the name of the table to modify
    - keys - String of the key name to set, or an iterable of keys of a particular type (if multiple)
    Optionals:
    - kType - String specifying type of key to set (primary/composite, unique, secondary)
    - ref - Iterable Size 2 (or an iterable of iterable size-2 if multiple references) specifying table and then column name(s)
    of reference table (refTable,refCol); refTable must be string, while refCol can be string or iterable of string (if multiple
    columns). More details provided below. Required for foreign keys, NoneType for all else.
    - engine - SQL engine to connect to database to execute query, if provided
    - echo - Echo output/actions from function

    Type of keys/constraints settable:
    - Primary (kType = 'primary', using one key)
    - Composite (kType = 'primary', using multiple keys (list))
    - Foreign (kType = 'foreign', requires the reference information inputted in ref). Note: the referenced column must be declared unique
    - Unique (kType = 'unique')
    - Secondary (kType = 'secondary')
    - Multi-Secondary (kType = 'secondary', using multiple keys).


    Note: Creating a unique or primary key automatically creates an index (secondary key)

    Note: Using multiple keys (in an iterable) for any key type always creates a composite key, with foreign keys having an exception.
    For foreign keys the rule is:
    - For all iterations (all refTables), the corresponding refCols must all be the same size (all size 1, 2, 3 etc..)
    - The number of keys must be the same as the number of reference points, and/or the same size as each refCol.
    - If each refCol is size 1, then non-composite keys are made, the same as the number of reference points (i.e. refTable count, or
    size of iterable((refTable,refCol))).
    - If each refCol is same size as keys, then composite keys are made, same as the number of reference points.
    """
    
    # Create wall of string (docstring) to use with sqlalchemy's text
    wallstring = ""

    ### Check first key type, then confirm key input (string or iterable of string)
    # Guard function against bad input (guarantees string or iterable of string)
    if not all(isinstance(key, str) for key in keys): raise KeyError("All keys must be in string")
    
    if kType.lower() == 'primary': # Create primary/composite key
        addkeys = ""

        # If single string input (not iterable of string)
        if isinstance(keys,str): 
            if echo: print(f'Setting Primary Key for table {tableName}')
            addkeys = '"' + keys + '"'
            
        else: # Iterable of string input
            if echo: print(f'Setting Composite (Primary) Key for table {tableName}')
            i = 0
            for key in keys:
                if i == 0:
                    i = 1
                    addkeys = '"' + key + '"'
                else:
                    addkeys += ', "' + key + '"'
        
        wallstring = f"""
                        ALTER TABLE \"{tableName}\"
                        DROP CONSTRAINT IF EXISTS \"{tableName}_pkey\" CASCADE;

                        ALTER TABLE \"{tableName}\"
                        ADD PRIMARY KEY ({addkeys});
                        """
        
    elif kType.lower() == 'foreign': # Create foreign key
        if ref is None: raise ValueError("Reference data (ref) should not be NoneType")  

        # If single reference set (not iterable of references), non-composite key
        if isinstance(keys,str) and all(isinstance(refset, str) for refset in ref) and len(ref) == 2:
            if echo: print(f'Setting Foreign Key for table {tableName} using references')
            
            refTable = ref[0]
            refCol = ref[1]

            wallstring = f"""
                        ALTER TABLE \"{tableName}\"
                        DROP CONSTRAINT IF EXISTS \"fk_{tableName}_{keys}_{refTable}_{refCol}\" CASCADE,
                        ADD CONSTRAINT \"fk_{tableName}_{keys}_{refTable}_{refCol}\"
                        FOREIGN KEY (\"{keys}\") REFERENCES \"{refTable}\"(\"{refCol}\") ON UPDATE CASCADE;
                        """

        # Concatenate each reference set change into one set of queries (multiple non-composite keys)
        elif all( (    all(isinstance(val, str) for val in refset)    and    len(refset) == 2 ) for refset in ref )    and    len(keys) == len(ref):
            if echo: print(f'Setting multiple Foreign Keys for table {tableName} using references')

            for i in range(len(ref)): # Note: len(ref) is same as len(keys) (we are sure keys is iterable of string)
                key = keys[i]
                refset = ref[i]
                refTable = refset[0]
                refCol = refset[1]
        
                wallstring += f"""
                                ALTER TABLE \"{tableName}\"
                                DROP CONSTRAINT IF EXISTS \"fk_{tableName}_{key}_{refTable}_{refCol}\" CASCADE,
                                ADD CONSTRAINT \"fk_{tableName}_{key}_{refTable}_{refCol}\"
                                FOREIGN KEY (\"{key}\") REFERENCES \"{refTable}\"(\"{refCol}\") ON UPDATE CASCADE;
                                """
        # If single reference set but iterable refCols, single composite key
        # This scenario: Match all given keys to all given refCols in the given refTable
        elif isinstance(ref[0],str)    and    all(isinstance(val,str) for val in ref[1])    and     len(ref) == 2    and    len(ref[1]) == len(keys):
            if echo: print(f'Setting a composite Foreign Key for table {tableName} using references')
            
            refTable = ref[0]
            refCols = ref[1]

            refColSeq = str(tuple(refCols)).replace("'", '"').replace(",)", ')')
            keySeq = str(tuple(keys)).replace("'", '"').replace(",)", ')')
            keylistname = ""
            refColname = ""
            for i in range(len(refCols)): # Note: len(refCols) is the same size as len(keys)
                col = refCols[i]
                key = keys[i]
                
                if i == 0:
                    keylistname = key
                    refColname = col
                else:
                    keylistname += "-" + key
                    refColname += "-" + col
    
            wallstring += f"""
                            ALTER TABLE \"{tableName}\"
                            DROP CONSTRAINT IF EXISTS \"fk_{tableName}_{keylistname}_{refTable}_{refColname}\" CASCADE,
                            ADD CONSTRAINT \"fk_{tableName}_{keylistname}_{refTable}_{refColname}\"
                            FOREIGN KEY {keySeq} REFERENCES \"{refTable}\" {refColSeq} ON UPDATE CASCADE;
                            """
        
        
        # Concatenate each reference set change into one set of queries (multiple composite keys)
        # This scenario: Match all given keys to all given refCols in each refTable
        elif (    all((    isinstance(refset[0],str)    and    all(isinstance(val, str) for val in refset[1])
                       and    len(refset) == 2 ) for refset in ref)    and    all(len(refset[1]) == len(keys) for refset in ref)    ):
            if echo: print(f'Setting multiple composite Foreign Keys for table {tableName} using references')

            keySeq = str(tuple(keys)).replace("'", '"').replace(",)", ')')
            keylistname = ""
            for i in range(len(ref)): # Note: len(refCols) is the same size as len(keys) (and ref is iterable(string,iterable(string)) )
                refset = ref[i]
                refTable = refset[0]
                refCols = refset[1]

                refColSeq = str(tuple(refCols)).replace("'", '"').replace(",)", ')')

                refColname = ""
                for j in range(len(refCols)):
                    col = refCols[j]
                    key = keys[j]
                    
                    if j == 0:
                        keylistname = key
                        refColname = col
                    else:
                        keylistname += "-" + key
                        refColname += "-" + col
        
                wallstring += f"""
                                ALTER TABLE \"{tableName}\"
                                DROP CONSTRAINT IF EXISTS \"fk_{tableName}_{keylistname}_{refTable}_{refColname}\" CASCADE,
                                ADD CONSTRAINT \"fk_{tableName}_{keylistname}_{refTable}_{refColname}\"
                                FOREIGN KEY {keySeq} REFERENCES \"{refTable}\" {refColSeq} ON UPDATE CASCADE;
                                """
        else: raise ValueError("Invalid reference data")
    
    
    elif kType.lower() == 'unique': # Create unique 'key' (constraint)
        if echo: print(f'Setting Unique Key(s) constraint(s) for table {tableName}')
        

        # If single string input (not iterable of string)
        if isinstance(keys,str): 
            wallstring = f"""
                        ALTER TABLE \"{tableName}\"
                        DROP CONSTRAINT IF EXISTS \"unique_{tableName}_{keys}\" CASCADE,
                        ADD CONSTRAINT \"unique_{tableName}_{keys}\" UNIQUE (\"{keys}\");
                        """
        else:
            # Concatenate each key change into one set of queries
            keySeq = str(tuple(keys)).replace("'", '"').replace(",)", ')')
            i = 0
            keylistname = ""
            for key in keys:
                if i == 0:
                    i = 1
                    keylistname = key
                else:
                    keylistname += "-" + key
                
            wallstring += f"""
                            ALTER TABLE \"{tableName}\"
                            DROP CONSTRAINT IF EXISTS \"unique_{tableName}_{keylistname}\" CASCADE,
                            ADD CONSTRAINT \"unique_{tableName}_{keylistname}\" UNIQUE {keySeq};
                            """
        

    elif kType.lower() == 'secondary': # Create secondary 'key' (index)
        if echo: print(f'Setting Secondary Key(s)/Indices for table {tableName}')

        # If single string input (not iterable of string)
        if isinstance(keys,str): 
            wallstring = f"""
                        DROP INDEX IF EXISTS \"ix_{tableName}_{keys}\" CASCADE;
                        CREATE INDEX \"ix_{tableName}_{keys}\" ON \"{tableName}\" (\"{keys}\");
                        """ 
        
        else:
            # Concatenate each key change into one set of queries
            keylistname = ""
            keySeq = str(tuple(keys)).replace("'", '"').replace(",)", ')')
            i = 0
            for key in keys:
                if i == 0:
                    i = 1
                    keylistname = key
                else:
                    keylistname += "-" + key
                
            wallstring += f"""
                        DROP INDEX IF EXISTS \"ix_{tableName}_{keylistname}\" CASCADE;
                        CREATE INDEX \"ix_{tableName}_{keylistname}\" ON \"{tableName}\" {keySeq};
                        """           
    else:
        # Guard function against bad input
        raise KeyError("Inputted key type (kType) must be 'primary', 'foreign', 'unique' or 'secondary'.")

    # Execute alteration
    if engine:
        if echo: print('Committing key set')
        ExecuteSQL(wallstring, engine)
            
    return wallstring

# Provide (or execute) query for dropping key(s) for a table (without replacing with another one)
def DropKeysQuery(tableName, keys = None, kType = 'primary', ref = None, engine = None, echo = False):
    """
    Provides query to delete the keys of a table in an SQL database. You must specify the sort of key being dropped
    (default is primary) and the key name (column(s)) and additional reference data, if applicable. Uses postgreSQL dialect.
    You must have an existing valid SQL engine (i.e. self.SQLengine) connection for this to function.
    This method outputs SQL queries (string) to drop the key.
    
    Input is as follows:
    - tableName - String of the name of the table to modify
    Optionals:
    - keys - String of the key name to drop, or an iterable of keys of a particular type (if multiple), not required for primary/composite keys.
    - kType - String specifying type of key to drop (primary/composite, unique, secondary)
    - ref - Iterable Size 2 (or an iterable of iterable size-2 if multiple) specifying table and then column name of reference table in string
    in form (refTable,refCol). Required for foreign keys, NoneType for all else
    - engine - SQL engine to connect to database to execute query, if provided
    - echo - Echo output/actions from function

    Type of keys/constraints that can be dropped:
    - Primary/Composite (kType = 'primary', does not require keys/key names)
    - Foreign (kType = 'foreign', requires the reference information inputted in ref).
    - Unique (kType = 'unique')
    - Secondary (kType='secondary')
    """
    
    # Create wall of string (docstring) to use with sqlalchemy's text
    wallstring = ""

    ### Check first key type, then confirm key input (string or iterable of string)
    # Guard function against bad input (guarantees string or iterable of string)
    if kType.lower() != 'primary' and not all(isinstance(key, str) for key in keys): raise KeyError("All keys must be in string")

    if kType.lower() == 'primary': # Create primary/composite key
        if echo: print(f'Dropping Primary/Composite Key for table {tableName}')

        wallstring = f"""
                        ALTER TABLE \"{tableName}\"
                        DROP CONSTRAINT IF EXISTS \"{tableName}_pkey\" CASCADE;
                        """
        
    elif kType.lower() == 'foreign': # Create foreign key
        if ref is None: raise ValueError("Reference data (ref) should not be NoneType")  

        # If single reference set (not iterable of references), non-composite key
        if isinstance(keys,str) and all(isinstance(refset, str) for refset in ref) and len(ref) == 2:
            if echo: print(f'Dropping Foreign Key for table {tableName} using references')
            
            refTable = ref[0]
            refCol = ref[1]

            wallstring = f"""
                        ALTER TABLE \"{tableName}\"
                        DROP CONSTRAINT IF EXISTS \"fk_{tableName}_{keys}_{refTable}_{refCol}\" CASCADE;
                        """

        # Concatenate each reference set change into one set of queries (multiple non-composite keys)
        elif all( (    all(isinstance(val, str) for val in refset)    and    len(refset) == 2 ) for refset in ref )    and    len(keys) == len(ref):
            if echo: print(f'Dropping multiple Foreign Keys for table {tableName} using references')

            for i in range(len(ref)): # Note: len(ref) is same as len(keys) (we are sure keys is iterable of string)
                key = keys[i]
                refset = ref[i]
                refTable = refset[0]
                refCol = refset[1]
        
                wallstring += f"""
                                ALTER TABLE \"{tableName}\"
                                DROP CONSTRAINT IF EXISTS \"fk_{tableName}_{key}_{refTable}_{refCol}\" CASCADE;
                                """
        # If single reference set but iterable refCols, single composite key
        # This scenario: Match all given keys to all given refCols in the given refTable
        elif isinstance(ref[0],str)    and    all(isinstance(val,str) for val in ref[1])    and     len(ref) == 2    and    len(ref[1]) == len(keys):
            if echo: print(f'Dropping a composite Foreign Key for table {tableName} using references')
            
            refTable = ref[0]
            refCols = ref[1]

            keylistname = ""
            refColname = ""
            for i in range(len(refCols)): # Note: len(refCols) is the same size as len(keys)
                col = refCols[i]
                key = keys[i]
                
                if i == 0:
                    keylistname = key
                    refColname = col
                else:
                    keylistname += "-" + key
                    refColname += "-" + col
    
            wallstring += f"""
                            ALTER TABLE \"{tableName}\"
                            DROP CONSTRAINT IF EXISTS \"fk_{tableName}_{keylistname}_{refTable}_{refColname}\" CASCADE;
                            """
        
        
        # Concatenate each reference set change into one set of queries (multiple composite keys)
        # This scenario: Match all given keys to all given refCols in each refTable
        elif (    all((    isinstance(refset[0],str)    and    all(isinstance(val, str) for val in refset[1])
                       and    len(refset) == 2 ) for refset in ref)    and    all(len(refset[1]) == len(keys) for refset in ref)    ):
            if echo: print(f'Setting multiple composite Foreign Keys for table {tableName} using references')

            keylistname = ""
            for i in range(len(ref)): # Note: len(refCols) is the same size as len(keys) (and ref is iterable(string,iterable(string)) )
                refset = ref[i]
                refTable = refset[0]
                refCols = refset[1]
                refColname = ""
                for j in range(len(refCols)):
                    col = refCols[j]
                    key = keys[j]
                    
                    if j == 0:
                        keylistname = key
                        refColname = col
                    else:
                        keylistname += "-" + key
                        refColname += "-" + col
        
                wallstring += f"""
                                ALTER TABLE \"{tableName}\"
                                DROP CONSTRAINT IF EXISTS \"fk_{tableName}_{keylistname}_{refTable}_{refColname}\" CASCADE;
                                """
        else: raise ValueError("Invalid reference data")
    
    
    elif kType.lower() == 'unique': # Create unique 'key' (constraint)
        if echo: print(f'Dropping unique key(s) constraints for table {tableName}')
        

        # If single string input (not iterable of string)
        if isinstance(keys,str): 
            wallstring = f"""
                            ALTER TABLE \"{tableName}\"
                            DROP CONSTRAINT IF EXISTS \"unique_{tableName}_{keys}\" CASCADE;
                            """
        else:
            # Concatenate each key change into one set of queries
            i = 0
            keylistname = ""
            for key in keys:
                if i == 0:
                    i = 1
                    keylistname = key
                else:
                    keylistname += "-" + key
                
            wallstring += f"""
                            ALTER TABLE \"{tableName}\"
                            DROP CONSTRAINT IF EXISTS \"unique_{tableName}_{keylistname}\" CASCADE;
                            """

    elif kType.lower() == 'secondary': # Create secondary 'key' (index)
        if echo: print(f'Dropping Secondary Key(s)/Indices for table {tableName}')

        # If single string input (not iterable of string)
        if isinstance(keys,str): 
            wallstring += f"""
                            DROP INDEX IF EXISTS \"ix_{tableName}_{keys}\" CASCADE;
                            """
        else:
            # Concatenate each key change into one set of queries
            keylistname = ""
            i = 0
            for key in keys:
                if i == 0:
                    i = 1
                    keylistname = key
                else:
                    keylistname += "-" + key
                
            wallstring += f"""
                        DROP INDEX IF EXISTS \"ix_{tableName}_{keylistname}\" CASCADE;
                        """           
    else:
        # Guard function against bad input
        raise KeyError("Inputted key type (kType) must be 'primary', 'foreign', 'unique' or 'secondary'.")
    
    if engine:
        if echo: print('Committing key drop')
        ExecuteSQL(wallstring, engine)

    return wallstring

# Execute SQL query
def ExecuteSQL(query, engine, fetch = False):
    """
    Executes a given query (in string) in SQL using the given connection engine.
    Enabling fetching provides the output from SQL (in whatever format it may be).
    Note that if fetch is enabled but no output is obtained it will raise an error.
    """
    from sqlalchemy import text

    with engine.connect() as conn:
        result = conn.execute(
            text( query )
        )
        if fetch:
            rows = result.fetchall()
            conn.commit()  # Required for Data Definiton Language (DDL) statements
            return rows
        else:
            conn.commit()  # Required for Data Definiton Language (DDL) statements
            return

# Applying upsert (update-insert) in postgres database
def postgres_upsert(table, conn, keys, data_iter):
    from sqlalchemy.dialects.postgresql import insert

    conststr = f"{table.table.name}_pkey"

    data = [dict(zip(keys, row)) for row in data_iter]

    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        constraint=conststr,
        set_={c.key: c for c in insert_statement.excluded},
    )
    conn.execute(upsert_statement)

# Every ticker is unique as alphavantage only provides US-based equities
# Note: for upsert to be applied, the table must have a primary key
 