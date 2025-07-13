
# MAKING CODE HERE TO SUBSTITUTE INTO PANDAS AS ID FOR RELATIONAL TABLES

# THINKING OF ADDING ATTRIBUTES SELF.RELATION.TICKER/MANIFEST/CALENDAR/ID (TO DATAMANAGER)

class SQLManager():
    """Placeholder class for package-level structure or future use."""
    pass


####### CONVERTING MANIFEST TABLE INTO SUITABLE SQL FORM, AND CONVERTING BACK TO PANDAS FORM (CONSIDERING TO KEEP MANIFESTID DATA)
# In general, we add the uniqueID columns as replacements to break the database down into a relational form (use ID as int is more efficient)
# We modify table to remove unnecessary columns or rows, then add to database (replace or append)
# Then we set the datatype of columns and set the primary keys.

# Repair SQL table/keys/data
def SQLRepair(dataManifest = None, connEngine = None, dataRepair = True, echo = False):
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


# Method to provide query for setting key(s) for a table (after dropping existing one first)
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


# Method to provide query for dropping key(s) for a table (without replacing with another one)
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

def ExecuteSQL(query, engine):
    """ Executes a given query (in string) in SQL using the given connection engine. """
    from sqlalchemy import text

    with engine.connect() as conn:
        conn.execute(
            text( query )
        )
        conn.commit()  # Required for Data Definiton Language (DDL) statements
    return


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
 