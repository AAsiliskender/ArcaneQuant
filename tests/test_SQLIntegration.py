# Full Integraton Testing of DataManifest - SQLAlchemy - SQLManager - Pandas systems.

# 1) Tests the SQLExecute function with database (direct query) and pandas (read_sql_query function)
# Tests if a generic input provided, results in the expected changes as per input.
# 2) Conducts a full integration test in table generation and key setting capabilities that are used in
# other functions that create, repair and update DataManifest - SQL systems.

# All other functions used for DataManifest - SQL interaction are derived from functions/methods used here  

# Packages
import pytest
import pandas as pd
from datetime import time as dt_time
from tabulate import tabulate
import psycopg2
from sqlalchemy import create_engine, Engine, text
import sqlalchemy.exc as sqlexc
from sqlalchemy import types as sqltype

# My packages
from arcanequant.quantlib.DataManifestManager import DataManifest
from arcanequant.quantlib.SQLManager import ExecuteSQL, SetKeysQuery, DropKeysQuery

# need sqlalchemy (and docker imports, after learning to set up docker)

################################################################
##################### FULL INTEGRATION TEST ####################
################################################################
# Test by:
# Using some valid inputs twice (check for if exists removal error, should not happen)
# Setting a column as foreign of another key (which has a different datatype)
# Removing keys, leaving few remaining and check for their existence before teardown
# Check the output (query) is valid by comparing the output string with a preverified string

################ TEST QUERY AND EXPECTED RESULTS ###############
# Note: we test for each remaining constraints. We added 6 non-composite and 8 composite uniques
# from testTable1 to testTable3 for a total of 14, and removed 7 uniques, one from testTable1
# (already non-existent), so there should be 8 remaining unique keys across all tables. There
# should only be one remaining primary key, foreign key, and secondary key.

# Also, we test for where primary keys were used by checking which columns for which tables are
# nullable (as primary keys enforce non-nullability).

### Test inputs (query)
# Primary key search query
# Input
pkeyQuery1 = """
    SELECT
        kcu.column_name
    FROM 
        information_schema.table_constraints AS tc
    JOIN 
        information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
    WHERE 
        tc.constraint_type = 'PRIMARY KEY'
        AND tc.table_name = 'testTable1';
    """
pkeyQuery2 = """
    SELECT
        kcu.column_name
    FROM 
        information_schema.table_constraints AS tc
    JOIN 
        information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
    WHERE 
        tc.constraint_type = 'PRIMARY KEY'
        AND tc.table_name = 'testTable2';
    """
pkeyQuery3 = """
    SELECT
        kcu.column_name
    FROM 
        information_schema.table_constraints AS tc
    JOIN 
        information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
    WHERE 
        tc.constraint_type = 'PRIMARY KEY'
        AND tc.table_name = 'testTable3';
    """
# Expected Result
pkeyExpected1 = """+---------------+
| column_name   |
|---------------|
| Int           |
+---------------+"""

pkeyExpected2 = """+---------------+
| column_name   |
|---------------|
+---------------+"""

pkeyExpected3 = """+---------------+
| column_name   |
|---------------|
+---------------+"""

# Foreign key search query
# Input
fkeyQuery1 = """
    SELECT
        tc.constraint_type,
        tc.constraint_name,
        kcu.column_name,
        ccu.table_name AS foreign_table,
        ccu.column_name AS foreign_column
    FROM 
        information_schema.table_constraints AS tc
    JOIN 
        information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
    LEFT JOIN 
        information_schema.constraint_column_usage AS ccu
        ON ccu.constraint_name = tc.constraint_name
    WHERE
        tc.constraint_type = 'FOREIGN KEY' AND
        tc.table_name = 'testTable1';
    """
fkeyQuery2 = """
    SELECT
        tc.constraint_type,
        tc.constraint_name,
        kcu.column_name,
        ccu.table_name AS foreign_table,
        ccu.column_name AS foreign_column
    FROM 
        information_schema.table_constraints AS tc
    JOIN 
        information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
    LEFT JOIN 
        information_schema.constraint_column_usage AS ccu
        ON ccu.constraint_name = tc.constraint_name
    WHERE
        tc.constraint_type = 'FOREIGN KEY' AND
        tc.table_name = 'testTable2';
    """
fkeyQuery3 = """
    SELECT
        tc.constraint_type,
        tc.constraint_name,
        kcu.column_name,
        ccu.table_name AS foreign_table,
        ccu.column_name AS foreign_column
    FROM 
        information_schema.table_constraints AS tc
    JOIN 
        information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
    LEFT JOIN 
        information_schema.constraint_column_usage AS ccu
        ON ccu.constraint_name = tc.constraint_name
    WHERE
        tc.constraint_type = 'FOREIGN KEY' AND
        tc.table_name = 'testTable3';
    """
# Expected Result
fkeyExpected1 = """+-------------------+-------------------+---------------+-----------------+------------------+
| constraint_type   | constraint_name   | column_name   | foreign_table   | foreign_column   |
|-------------------+-------------------+---------------+-----------------+------------------|
+-------------------+-------------------+---------------+-----------------+------------------+"""

fkeyExpected2 = """+-------------------+----------------------------------------------------------+---------------+-----------------+------------------+
| constraint_type   | constraint_name                                          | column_name   | foreign_table   | foreign_column   |
|-------------------+----------------------------------------------------------+---------------+-----------------+------------------|
| FOREIGN KEY       | fk_testTable2_SmallInt-Float1_testTable1_SmallInt-Float1 | SmallInt      | testTable1      | SmallInt         |
| FOREIGN KEY       | fk_testTable2_SmallInt-Float1_testTable1_SmallInt-Float1 | SmallInt      | testTable1      | Float1           |
| FOREIGN KEY       | fk_testTable2_SmallInt-Float1_testTable1_SmallInt-Float1 | Float1        | testTable1      | SmallInt         |
| FOREIGN KEY       | fk_testTable2_SmallInt-Float1_testTable1_SmallInt-Float1 | Float1        | testTable1      | Float1           |
| FOREIGN KEY       | fk_testTable2_SmallInt-Float1_testTable3_SmallInt-Float1 | SmallInt      | testTable3      | SmallInt         |
| FOREIGN KEY       | fk_testTable2_SmallInt-Float1_testTable3_SmallInt-Float1 | SmallInt      | testTable3      | Float1           |
| FOREIGN KEY       | fk_testTable2_SmallInt-Float1_testTable3_SmallInt-Float1 | Float1        | testTable3      | SmallInt         |
| FOREIGN KEY       | fk_testTable2_SmallInt-Float1_testTable3_SmallInt-Float1 | Float1        | testTable3      | Float1           |
+-------------------+----------------------------------------------------------+---------------+-----------------+------------------+"""

fkeyExpected3 = """+-------------------+-------------------+---------------+-----------------+------------------+
| constraint_type   | constraint_name   | column_name   | foreign_table   | foreign_column   |
|-------------------+-------------------+---------------+-----------------+------------------|
+-------------------+-------------------+---------------+-----------------+------------------+"""

# Unique key search query
# Input
uniqueQuery1 = """
    SELECT
        tc.constraint_type,
        tc.constraint_name,
        kcu.column_name
    FROM 
        information_schema.table_constraints AS tc
    JOIN 
        information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
    LEFT JOIN 
        information_schema.constraint_column_usage AS ccu
        ON ccu.constraint_name = tc.constraint_name
    WHERE
        tc.constraint_type = 'UNIQUE' AND
        tc.table_name = 'testTable1';
    """
uniqueQuery2 = """
    SELECT
        tc.constraint_type,
        tc.constraint_name,
        kcu.column_name
    FROM 
        information_schema.table_constraints AS tc
    JOIN 
        information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
    LEFT JOIN 
        information_schema.constraint_column_usage AS ccu
        ON ccu.constraint_name = tc.constraint_name
    WHERE
        tc.constraint_type = 'UNIQUE' AND
        tc.table_name = 'testTable2';
    """
uniqueQuery3 = """
    SELECT
        tc.constraint_type,
        tc.constraint_name,
        kcu.column_name
    FROM 
        information_schema.table_constraints AS tc
    JOIN 
        information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
    LEFT JOIN 
        information_schema.constraint_column_usage AS ccu
        ON ccu.constraint_name = tc.constraint_name
    WHERE
        tc.constraint_type = 'UNIQUE' AND
        tc.table_name = 'testTable3';
    """
# Expected Result
uniqueExpected1 = """+-------------------+-----------------------------------+---------------+
| constraint_type   | constraint_name                   | column_name   |
|-------------------+-----------------------------------+---------------|
| UNIQUE            | unique_testTable1_SmallInt-Float1 | SmallInt      |
| UNIQUE            | unique_testTable1_SmallInt-Float1 | SmallInt      |
| UNIQUE            | unique_testTable1_SmallInt-Float1 | Float1        |
| UNIQUE            | unique_testTable1_SmallInt-Float1 | Float1        |
+-------------------+-----------------------------------+---------------+"""

uniqueExpected2 = """+-------------------+-----------------------------------+---------------+
| constraint_type   | constraint_name                   | column_name   |
|-------------------+-----------------------------------+---------------|
| UNIQUE            | unique_testTable2_SmallInt        | SmallInt      |
| UNIQUE            | unique_testTable2_BigInt          | BigInt        |
| UNIQUE            | unique_testTable2_BigInt-Time     | BigInt        |
| UNIQUE            | unique_testTable2_BigInt-Time     | BigInt        |
| UNIQUE            | unique_testTable2_BigInt-Time     | Time          |
| UNIQUE            | unique_testTable2_BigInt-Time     | Time          |
| UNIQUE            | unique_testTable2_SmallInt-Float1 | SmallInt      |
| UNIQUE            | unique_testTable2_SmallInt-Float1 | SmallInt      |
| UNIQUE            | unique_testTable2_SmallInt-Float1 | Float1        |
| UNIQUE            | unique_testTable2_SmallInt-Float1 | Float1        |
| UNIQUE            | unique_testTable2_SmallInt-Str    | SmallInt      |
| UNIQUE            | unique_testTable2_SmallInt-Str    | SmallInt      |
| UNIQUE            | unique_testTable2_SmallInt-Str    | Str           |
| UNIQUE            | unique_testTable2_SmallInt-Str    | Str           |
+-------------------+-----------------------------------+---------------+"""

uniqueExpected3 = """+-------------------+-----------------------------------+---------------+
| constraint_type   | constraint_name                   | column_name   |
|-------------------+-----------------------------------+---------------|
| UNIQUE            | unique_testTable3_SmallInt        | SmallInt      |
| UNIQUE            | unique_testTable3_BigInt          | BigInt        |
| UNIQUE            | unique_testTable3_SmallInt-Float1 | SmallInt      |
| UNIQUE            | unique_testTable3_SmallInt-Float1 | SmallInt      |
| UNIQUE            | unique_testTable3_SmallInt-Float1 | Float1        |
| UNIQUE            | unique_testTable3_SmallInt-Float1 | Float1        |
+-------------------+-----------------------------------+---------------+"""

# Secondary key (index) search query
# Input
ixQuery1 = """
    SELECT indexname
    FROM pg_indexes
    WHERE tablename = 'testTable1' AND indexname LIKE 'ix_%';
"""
ixQuery2 = """
    SELECT indexname
    FROM pg_indexes
    WHERE tablename = 'testTable2' AND indexname LIKE 'ix_%';
"""
ixQuery3 = """
    SELECT indexname
    FROM pg_indexes
    WHERE tablename = 'testTable3' AND indexname LIKE 'ix_%';
"""
# Expected Result
ixExpected1 = """+--------------------------+
| indexname                |
|--------------------------|
| ix_testTable1_Int-BigInt |
+--------------------------+"""

ixExpected2 = """+---------------------+
| indexname           |
|---------------------|
| ix_testTable2_Index |
+---------------------+"""

ixExpected3 = """+-------------+
| indexname   |
|-------------|
+-------------+"""

# Nullable column search query
# Input
nullQuery1 = """
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_name = 'testTable1';
    """
nullQuery2 = """
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_name = 'testTable2';
    """
nullQuery3 = """
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_name = 'testTable3';
    """
# Expected Result
nullExpected1 = """+---------------+------------------------+---------------+
| column_name   | data_type              | is_nullable   |
|---------------+------------------------+---------------|
| Time          | time without time zone | YES           |
| Int           | integer                | NO            |
| BigInt        | bigint                 | NO            |
| SmallInt      | smallint               | YES           |
| Float1        | double precision       | YES           |
| Float2        | double precision       | YES           |
| Str           | character varying      | YES           |
+---------------+------------------------+---------------+"""

nullExpected2 = """+---------------+------------------------+---------------+
| column_name   | data_type              | is_nullable   |
|---------------+------------------------+---------------|
| Index         | integer                | YES           |
| SmallInt      | smallint               | NO            |
| Int           | integer                | YES           |
| BigInt        | bigint                 | YES           |
| Time          | time without time zone | YES           |
| Float1        | double precision       | YES           |
| Float2        | double precision       | NO            |
| Str           | character varying      | YES           |
+---------------+------------------------+---------------+"""

nullExpected3 = """+---------------+------------------------+---------------+
| column_name   | data_type              | is_nullable   |
|---------------+------------------------+---------------|
| Time          | time without time zone | YES           |
| Int           | integer                | NO            |
| BigInt        | bigint                 | NO            |
| SmallInt      | smallint               | NO            |
| Float1        | double precision       | YES           |
| Float2        | double precision       | YES           |
| Str           | character varying      | YES           |
+---------------+------------------------+---------------+"""

testIntegrationInput = [(
    (pkeyQuery1, pkeyExpected1), (pkeyQuery2, pkeyExpected2), (pkeyQuery3, pkeyExpected3),
    (fkeyQuery1, fkeyExpected1), (fkeyQuery2, fkeyExpected2), (fkeyQuery3, fkeyExpected3),
    (uniqueQuery1, uniqueExpected1), (uniqueQuery2, uniqueExpected2), (uniqueQuery3, uniqueExpected3),
    (ixQuery1, ixExpected1), (ixQuery2, ixExpected2), (ixQuery3, ixExpected3),
    (nullQuery1, nullExpected1), (nullQuery2, nullExpected2), (nullQuery3, nullExpected3)
    )]

######################### TEST FUNCTION ########################
@pytest.mark.parametrize("testIntegrationParams", testIntegrationInput)
def test_SQLIntegration(testIntegrationParams, setup_SQLtestengine):
    """
    Tests the codebase-Pandas-SQLAlchemy-SQL setup fully integrated by using test engine created
    using my DataManifest class, as well as pandas to initialise a SQL table, and then use SetKeysQuery
    and DropKeysQuery to set and drop every type of key at least once (as errors should not pop up
    and kill the process at all), and some are also repeated to check for repeatability errors.
    Some keys are left over and these, as well as indication of which columns were used as
    primary/composite keys (via non-nullability) are checked via extracting SQL query output
    directly using SQLAlchemy.
    """

    testTable1 = pd.DataFrame(columns=["SmallInt","Int","BigInt","Str",'Float1','Float2','Time'])
    testTable1.rename_axis("Index",axis=0,inplace = True)
    testTable2 = pd.DataFrame(columns=['SmallInt','Int','BigInt','Str','Float1','Float2','Time'])
    testTable2.rename_axis("Index",axis=0,inplace = True)
    testTable3 = pd.DataFrame(columns=['SmallInt','Int','BigInt','Str','Float1','Float2','Time'])
    testTable3.rename_axis("Index",axis=0,inplace = True)
    
    # Set datatype of columns
    datatypes = {
            'Index': sqltype.Integer(),
            "SmallInt": sqltype.SmallInteger(),
            "Int": sqltype.Integer(),
            "BigInt": sqltype.BigInteger(),
            "Str": sqltype.String(),
            "Float1": sqltype.Float(),
            "Float2": sqltype.Float(),
            "Time": sqltype.Time()
        }

    testTable1.to_sql('testTable1', setup_SQLtestengine, if_exists='replace', index=False, dtype = datatypes)
    testTable2.to_sql('testTable2', setup_SQLtestengine, if_exists='replace', index=True, dtype = datatypes)
    testTable3.to_sql('testTable3', setup_SQLtestengine, if_exists='replace', index=False, dtype = datatypes)
    
    
    ######## Primary/composite key test ########
    ### Primary key
    PKS1 = SetKeysQuery('testTable1', 'BigInt', 'primary', engine = setup_SQLtestengine)
    PKD1 = DropKeysQuery('testTable1', 'BigInt', 'primary', engine = setup_SQLtestengine)
    # To check existence directly from output 
    PKS2 = SetKeysQuery('testTable1', 'Int', 'primary', engine = setup_SQLtestengine)

    ### Composite (primary) key
    CKS1 = SetKeysQuery('testTable2', ['SmallInt', 'Float2'], 'primary', engine = setup_SQLtestengine)
    CKD1 = DropKeysQuery('testTable2', ['SmallInt', 'Float2'], 'primary', engine = setup_SQLtestengine)
    # Use on testTable3 as we confirm which tables have been made primary through non-nullable values
    CKS2 = SetKeysQuery('testTable3', ['SmallInt', 'Int', 'BigInt'], 'primary', engine = setup_SQLtestengine)
    CKD2 = DropKeysQuery('testTable3', ['SmallInt', 'Int', 'BigInt'], 'primary', engine = setup_SQLtestengine)
    
    ######## Setting Unique key test ########
    ### Unique key
    NCUS1 = SetKeysQuery('testTable2', 'SmallInt', 'unique', engine = setup_SQLtestengine)
    NCUS2 = SetKeysQuery('testTable3', 'SmallInt', 'unique', engine = setup_SQLtestengine)
    NCUS3 = SetKeysQuery('testTable2', 'BigInt', 'unique', engine = setup_SQLtestengine)
    NCUS4 = SetKeysQuery('testTable3', 'BigInt', 'unique', engine = setup_SQLtestengine)
    NCUS5 = SetKeysQuery('testTable2', 'Time', 'unique', engine = setup_SQLtestengine)
    NCUS6 = SetKeysQuery('testTable2', 'Str', 'unique', engine = setup_SQLtestengine)
    
    ### Composite unique key
    CUS1 = SetKeysQuery('testTable2', ['BigInt', 'Time'], 'unique', engine = setup_SQLtestengine)
    CUS2 = SetKeysQuery('testTable3', ['BigInt', 'Time'], 'unique', engine = setup_SQLtestengine)
    CUS3 = SetKeysQuery('testTable3', ['BigInt', 'Float2'], 'unique', engine = setup_SQLtestengine)
    CUS4 = SetKeysQuery('testTable2', ['BigInt', 'Float1'], 'unique', engine = setup_SQLtestengine)
    CUS5 = SetKeysQuery('testTable2', ['SmallInt', 'Float1'], 'unique', engine = setup_SQLtestengine)
    CUS6 = SetKeysQuery('testTable2', ['SmallInt', 'Str'], 'unique', engine = setup_SQLtestengine)
    CUS7 = SetKeysQuery('testTable1', ['SmallInt', 'Float1'], 'unique', engine = setup_SQLtestengine)
    CUS8 = SetKeysQuery('testTable3', ['SmallInt', 'Float1'], 'unique', engine = setup_SQLtestengine)

    ######## Foreign key test ########
    ### Single non-composite foreign
    # Repeat to check for error
    SNCFS1 = SetKeysQuery('testTable1', 'BigInt', 'foreign', engine = setup_SQLtestengine, ref = ('testTable3','BigInt') )
    SNCFS2 = SetKeysQuery('testTable1', 'BigInt', 'foreign', engine = setup_SQLtestengine, ref = ('testTable3','BigInt') )
    SNCFD1 = DropKeysQuery('testTable1', 'BigInt', 'foreign', engine = setup_SQLtestengine, ref = ('testTable3','BigInt') )
    assert SNCFS1 == SNCFS2, "Repeatability Error, SNCFS1/2"

    ### Single composite foreign
    SCFS1 = SetKeysQuery('testTable2', ['BigInt', 'Time'], 'foreign', engine = setup_SQLtestengine, ref = ('testTable3',('BigInt','Time')) )
    SCFD1 = DropKeysQuery('testTable2', ['BigInt', 'Time'], 'foreign', engine = setup_SQLtestengine, ref = ('testTable3',('BigInt','Time')) )
    
    ### Multiple non-composite foreign
    MNCFS1 = SetKeysQuery('testTable1', ['SmallInt', 'BigInt'], 'foreign', engine = setup_SQLtestengine, ref = [('testTable2','SmallInt'),('testTable2','BigInt')])
    MNCFD1 = DropKeysQuery('testTable1', ['SmallInt', 'BigInt'], 'foreign', engine = setup_SQLtestengine, ref = [('testTable2','SmallInt'),('testTable2','BigInt')])
    
    # Multiple non-composite foreign but each ref different table
    MNCFS1_table = SetKeysQuery('testTable1', ['SmallInt', 'BigInt'], 'foreign', engine = setup_SQLtestengine, ref = [('testTable2','SmallInt'),('testTable3','BigInt')])
    MNCFD1_table = DropKeysQuery('testTable1', ['SmallInt', 'BigInt'], 'foreign', engine = setup_SQLtestengine, ref = [('testTable2','SmallInt'),('testTable3','BigInt')])
    
    ### Multiple composite foreign
    MCFS1 = SetKeysQuery('testTable1', ['BigInt', 'Time'], 'foreign', engine = setup_SQLtestengine, ref = [('testTable2',('BigInt','Time')),('testTable3',('BigInt','Time'))])
    # Repeat to check for error
    MCFD1 = DropKeysQuery('testTable1', ['BigInt', 'Time'], 'foreign', engine = setup_SQLtestengine, ref = [('testTable2',('BigInt','Time')),('testTable3',('BigInt','Time'))])
    MCFD2 = DropKeysQuery('testTable1', ['BigInt', 'Time'], 'foreign', engine = setup_SQLtestengine, ref = [('testTable2',('BigInt','Time')),('testTable3',('BigInt','Time'))])
    assert MCFD1 == MCFD2, "Repeatability Error, MCFD1/2"
    # Add extra MCF key to check at end
    MCFS2 = SetKeysQuery('testTable2', ['SmallInt', 'Float1'], 'foreign', engine = setup_SQLtestengine, ref = [('testTable1',('SmallInt','Float1')),('testTable3',('SmallInt','Float1'))])
    
    # Multiple composite foreign but different refCol name to key column
    MCFS_col1 = SetKeysQuery('testTable1', ['BigInt', 'Float1'], 'foreign', engine = setup_SQLtestengine, ref = [('testTable2',('BigInt','Float1')),('testTable3',('BigInt','Float2'))])
    MCFD_col1 = DropKeysQuery('testTable1', ['BigInt', 'Float1'], 'foreign', engine = setup_SQLtestengine, ref = [('testTable2',('BigInt','Float1')),('testTable3',('BigInt','Float2'))])
    
    # Multiple composite foreign but different datatype (and name)
    MCFS_dtype1 = SetKeysQuery('testTable1', ['BigInt', 'Float1'], 'foreign', engine = setup_SQLtestengine, ref = [('testTable2',('SmallInt','Float1')),('testTable3',('BigInt','Float2'))])
    MCFD_dtype1 = DropKeysQuery('testTable1', ['BigInt', 'Float1'], 'foreign', engine = setup_SQLtestengine, ref = [('testTable2',('SmallInt','Float1')),('testTable3',('BigInt','Float2'))])
    
    # Test and catch erroneous input (foreign key of a float into a string) (but no error for drop)
    try:
        MCFS_dtype2 = SetKeysQuery('testTable1', ['BigInt', 'Float1'], 'foreign', engine = setup_SQLtestengine, ref = [('testTable2',('SmallInt','Str')),('testTable3',('BigInt','Float2'))])
    except psycopg2.errors.DatatypeMismatch as e:
        print(f"Caught error: {type(e).__name__} - {e}")
    except sqlexc.ProgrammingError as e:  
        print(f"Caught SQLAlchemy ProgrammingError (could be DatatypeMismatch): {e}")
    except Exception as e:  
        print(f"Some other error occurred: {e}")
    MCFD_dtype2 = DropKeysQuery('testTable1', ['BigInt', 'Float1'], 'foreign', engine = setup_SQLtestengine, ref = [('testTable2',('SmallInt','Str')),('testTable3',('BigInt','Float2'))])
    
    # Multiple non-composite using composite form foreign
    MCFS1_NC = SetKeysQuery('testTable1', ['SmallInt'], 'foreign', engine = setup_SQLtestengine, ref = [('testTable2',['SmallInt']),('testTable3',['SmallInt'])])
    MCFD1_NC = DropKeysQuery('testTable1', ['SmallInt'], 'foreign', engine = setup_SQLtestengine, ref = [('testTable2',['SmallInt']),('testTable3',['SmallInt'])])
    
    ######## Dropping unique key test (dropping only some, was needed for foreign keys) ########
    # Also forms test as 8 unique keys should remain (14 - (7-1), explained below)
    UD1 = DropKeysQuery('testTable1', 'BigInt', 'unique', engine = setup_SQLtestengine)
    UD2 = DropKeysQuery('testTable2', 'Time', 'unique', engine = setup_SQLtestengine)
    UD3 = DropKeysQuery('testTable2', 'Str', 'unique', engine = setup_SQLtestengine)
    
    CUD1 = DropKeysQuery('testTable3', ['BigInt', 'Time'], 'unique', engine = setup_SQLtestengine)
    CUD2 = DropKeysQuery('testTable3', ['BigInt', 'Float2'], 'unique', engine = setup_SQLtestengine)
    CUD3 = DropKeysQuery('testTable2', ['BigInt', 'Float1'], 'unique', engine = setup_SQLtestengine)
    CUD3 = DropKeysQuery('testTable2', ['BigInt', 'Float1'], 'unique', engine = setup_SQLtestengine)
    
    ######## Secondary key test ########
    ### Secondary non-composite key
    SS1 = SetKeysQuery('testTable1', 'Int', 'secondary', engine = setup_SQLtestengine) 
    SD1 = DropKeysQuery('testTable1', 'Int', 'secondary', engine = setup_SQLtestengine)
    
    ### Secondary composite key
    SCS1 = SetKeysQuery('testTable1', ('Int','BigInt'), 'secondary', engine = setup_SQLtestengine)
    #SCD1 = DropKeysQuery('testTable1', ('Int','BigInt'), 'secondary', engine = setup_SQLtestengine)


    with setup_SQLtestengine.connect() as conn:

        for testParam in testIntegrationParams:
            extract = conn.execute(text(testParam[0]))
            rows = extract.fetchall()  # Fetch all rows
            headers = extract.keys() # Fetch all headers
            result = tabulate(rows, headers = headers, tablefmt="psql")

            assert result == testParam[1], f"Expected {testParam[1]} but got {result}"
            
#########################