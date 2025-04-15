# Integraton Testing of SQLExecute function with database (direct query) and pandas (read_sql_query function)
# Tests if a generic input provided, results in the expected changes as per input.

# TO DO TO COMPLETE THIS TEST:
# GENERATE A TEMPORARY DATABASE THAT DISAPPEARS AFTER SESSION OVER
# IMPORT CREATE ENGINE OR SOMETHING LIKE THAT

# SQL commands to create test user with necessary functions to be able to run tests
# This set of code needs to run only once on setup

# CREATE USER "testUser" 
# ALTER USER "testUser" WITH PASSWORD 'testPassword';
# ALTER DATABASE "testDatabase" OWNER TO "testUser";



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
#DROP TABLE IF EXISTS \"testTable1\" CASCADE;
testquery = f"""
                    CREATE TABLE \"testTable1\" (
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

                    INSERT INTO \"testTable1\" (\"Time\")
                    SELECT generate_series('1900-01-01 00:00:00'::TIMESTAMP, '1900-01-01 00:01:00'::TIMESTAMP, '10 second'::INTERVAL)::TIME;
                    """

sql_expected = """+----------+----------+--------+----------+----------+
| Time     |   TimeID |   Hour |   Minute |   Second |
|----------+----------+--------+----------+----------|
| 00:00:00 |        0 |      0 |        0 |        0 |
| 00:00:10 |       10 |      0 |        0 |       10 |
| 00:00:20 |       20 |      0 |        0 |       20 |
| 00:00:30 |       30 |      0 |        0 |       30 |
| 00:00:40 |       40 |      0 |        0 |       40 |
| 00:00:50 |       50 |      0 |        0 |       50 |
| 00:01:00 |      100 |      0 |        1 |        0 |
+----------+----------+--------+----------+----------+"""

pd_expected = [[dt_time(0), 0, 0, 0, 0],
 [dt_time(0, 0, 10), 10, 0, 0, 10],
 [dt_time(0, 0, 20), 20, 0, 0, 20],
 [dt_time(0, 0, 30), 30, 0, 0, 30],
 [dt_time(0, 0, 40), 40, 0, 0, 40],
 [dt_time(0, 0, 50), 50, 0, 0, 50],
 [dt_time(0, 1), 100, 0, 1, 0]]


testParams = [('Pandas', testquery, pd_expected), ('Direct', testquery, sql_expected)]

# The result is read by using SQLAlchemy connect_engine provided connection, and separately using pandas
@pytest.mark.parametrize("testFrom,queryinput,expected", testParams)
def test_ExecuteSQL(testFrom, queryinput, expected, setup_SQLtestengine):

    ExecuteSQL(queryinput, setup_SQLtestengine)

    # Extract what the table should look like and compare to what it should be
    # For example, from SQL its a string of the table appearance and contents
    # From Pandas it is the values within the table being checked
    with setup_SQLtestengine.connect() as conn:
        if testFrom == 'Pandas':
            result = pd.read_sql_query("SELECT * FROM \"testTable1\";", conn)
            assert result.values.tolist() == expected
            
        
        elif testFrom == 'Direct':
            extract = conn.execute(text("SELECT * FROM \"testTable1\";"))

            rows = extract.fetchall()  # Fetch all rows
            headers = extract.keys() # Fetch all headers

            result = tabulate(rows, headers=headers, tablefmt="psql")
            assert result == expected

################## ALL TEST CASES (check sql directly to confirm)
# Test by:
# Using some valid inputs twice (check for if exists removal error, should not happen)
# Setting a column as foreign of another key (which has a different datatype)
# Setting foreign key on a non-unique ref column (catch error)
# Check the output (commit string) is valid by comparing with a premade string
# Removing tables with keys remaining

# TO USE ERROR ASSERTION AND EQUAL ASSERTIONS

def test_SQLIntegration(setup_SQLtestengine):
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

    #teardownquery = f"""
    #                    DROP TABLE IF EXISTS \"testTable1\" CASCADE;
    #                    DROP TABLE IF EXISTS \"testTable2\" CASCADE;
    #                    DROP TABLE IF EXISTS \"testTable3\" CASCADE;
    #                """
    #ExecuteSQL(text(teardownquery))

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


    ################################################################
    ################ TEST QUERY AND EXPECTED RESULTS ###############
    ################################################################
    # Note: we test for each remaining constraints. We added 6 non-composite and 8 composite uniques from testTable1 to testTable3
    # for a total of 14, and removed 7 uniques, one from testTable1 (already non-existent), so there
    # should be 8 remaining unique keys across all tables. There should only be one remaining primary
    # key, foreign key, and secondary key.

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
#    ixExpected1 = 

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


    with setup_SQLtestengine.connect() as conn:

        # Primary key test result
        pkeyExtract1 = conn.execute(text(pkeyQuery1))
        pkeyExtract2 = conn.execute(text(pkeyQuery2))
        pkeyExtract3 = conn.execute(text(pkeyQuery3))
        pkeyRows1 = pkeyExtract1.fetchall()  # Fetch all rows
        pkeyRows2 = pkeyExtract2.fetchall()
        pkeyRows3 = pkeyExtract3.fetchall()
        pkeyHeaders1 = pkeyExtract1.keys() # Fetch all headers
        pkeyHeaders2 = pkeyExtract2.keys()
        pkeyHeaders3 = pkeyExtract3.keys()
        pkeyResult1 = tabulate(pkeyRows1, headers = pkeyHeaders1, tablefmt="psql")
        pkeyResult2 = tabulate(pkeyRows2, headers = pkeyHeaders2, tablefmt="psql")
        pkeyResult3 = tabulate(pkeyRows3, headers = pkeyHeaders3, tablefmt="psql")
        print(pkeyResult1)
        print(pkeyResult2)
        print(pkeyResult3)


        fkeyExtract1 = conn.execute(text(fkeyQuery1))
        fkeyExtract2 = conn.execute(text(fkeyQuery2))
        fkeyExtract3 = conn.execute(text(fkeyQuery3))
        fkeyRows1 = fkeyExtract1.fetchall() # Fetch all rows
        fkeyRows2 = fkeyExtract2.fetchall()
        fkeyRows3 = fkeyExtract3.fetchall()
        fkeyHeaders1 = fkeyExtract1.keys() # Fetch all headers
        fkeyHeaders2 = fkeyExtract1.keys()
        fkeyHeaders3 = fkeyExtract1.keys()
        fkeyResult1 = tabulate(fkeyRows1, headers = fkeyHeaders1, tablefmt="psql")
        fkeyResult2 = tabulate(fkeyRows2, headers = fkeyHeaders2, tablefmt="psql")
        fkeyResult3 = tabulate(fkeyRows3, headers = fkeyHeaders3, tablefmt="psql")
        print(fkeyResult1)
        print(fkeyResult2)
        print(fkeyResult3)
        
        ixExtract1 = conn.execute(text(ixQuery1))
        ixExtract2 = conn.execute(text(ixQuery2))
        ixExtract3 = conn.execute(text(ixQuery3))
        ixRows1 = ixExtract1.fetchall() # Fetch all rows
        ixRows2 = ixExtract2.fetchall()
        ixRows3 = ixExtract3.fetchall()
        ixHeaders1 = ixExtract1.keys() # Fetch all headers
        ixHeaders2 = ixExtract2.keys()
        ixHeaders3 = ixExtract3.keys()
        ixResult1 = tabulate(ixRows1, headers = ixHeaders1, tablefmt="psql")
        ixResult2 = tabulate(ixRows2, headers = ixHeaders2, tablefmt="psql")
        ixResult3 = tabulate(ixRows3, headers = ixHeaders3, tablefmt="psql")
        print(ixResult1)
        print(ixResult2)
        print(ixResult3)
        
        uniqueExtract1 = conn.execute(text(uniqueQuery1))
        uniqueExtract2 = conn.execute(text(uniqueQuery2))
        uniqueExtract3 = conn.execute(text(uniqueQuery3))
        uniqueRows1 = uniqueExtract1.fetchall() # Fetch all rows
        uniqueRows2 = uniqueExtract2.fetchall()
        uniqueRows3 = uniqueExtract3.fetchall()
        uniqueHeaders1 = uniqueExtract1.keys() # Fetch all headers
        uniqueHeaders2 = uniqueExtract2.keys()
        uniqueHeaders3 = uniqueExtract3.keys()
        uniqueResult1 = tabulate(uniqueRows1, headers = uniqueHeaders1, tablefmt="psql")
        uniqueResult2 = tabulate(uniqueRows2, headers = uniqueHeaders2, tablefmt="psql")
        uniqueResult3 = tabulate(uniqueRows3, headers = uniqueHeaders3, tablefmt="psql")
        print(uniqueResult1)
        print(uniqueResult2)
        print(uniqueResult3)


        nullExtract1 = conn.execute(text(nullQuery1))
        nullExtract2 = conn.execute(text(nullQuery2))
        nullExtract3 = conn.execute(text(nullQuery3))
        nullRows1 = nullExtract1.fetchall() # Fetch all rows
        nullRows2 = nullExtract2.fetchall()
        nullRows3 = nullExtract3.fetchall()
        nullHeaders1 = nullExtract1.keys() # Fetch all headers
        nullHeaders2 = nullExtract2.keys()
        nullHeaders3 = nullExtract3.keys()
        nullResult1 = tabulate(nullRows1, headers = nullHeaders1, tablefmt="psql")
        nullResult2 = tabulate(nullRows2, headers = nullHeaders2, tablefmt="psql")
        nullResult3 = tabulate(nullRows3, headers = nullHeaders3, tablefmt="psql")
        print(nullResult1)
        print(nullResult2)
        print(nullResult3)

        #assert result == expected


    # WHAT DO WE CHECK HERE? IF TABLE IS EMPTY?

#########################



# TESTING MERGE HERE (FOR WHEN EXTRACTING FROM DATABASE)



@pytest.fixture(scope = 'function')
def setup_SQLtestengine() -> Engine:
    # Setup
    setupManifest = DataManifest()
    setupManifest.connectSQL('testSQLlogin')

    try: # Used to teardown no matter the error/failure
        yield setupManifest.SQLengine
    finally:
        # Teardown
        teardownquery = f"""
                            DROP TABLE IF EXISTS \"testTable1\" CASCADE;
                            DROP TABLE IF EXISTS \"testTable2\" CASCADE;
                            DROP TABLE IF EXISTS \"testTable3\" CASCADE;
                        """
        with setupManifest.SQLengine.connect() as conn:        
            teardown = conn.execute(text(teardownquery))
            conn.commit()
