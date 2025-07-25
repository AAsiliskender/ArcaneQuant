# Integration Testing of SQLExecute function with database (direct query) and pandas (read_sql_query function)
# Tests if a generic input provided, results in the expected changes as per input.

# Packages
import pytest
import pandas as pd
from datetime import time as dt_time
from tabulate import tabulate
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine, Engine, text
import sqlalchemy.exc as sqlexc
from sqlalchemy import types as sqltype

# My packages
from arcanequant.quantlib.DataManifestManager import DataManifest
from arcanequant.quantlib.SQLManager import ExecuteSQL, SetKeysQuery, DropKeysQuery

# need sqlalchemy (and docker imports, after learning to set up docker)

################################################################
###################### SQL EXECUTION TEST ######################
################################################################
# Test by:
# Creating a defined table, and reading table output
# The result is tested twice, once by using SQLAlchemy connect_engine provided connection to
# get output, and once separately by using pandas.

################ TEST QUERY AND EXPECTED RESULTS ###############
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

######################### TEST FUNCTION ########################
@pytest.mark.parametrize("testFrom,queryinput,expected", testParams)
def test_ExecuteSQL(testFrom: str, queryinput: str, expected, setup_SQLtestengine: sqlalchemy.engine):

    ExecuteSQL(queryinput, setup_SQLtestengine) # Execute the test input

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

#########################

##################### FIXTURE FUNCTION(S) ######################
@pytest.fixture(scope = 'function') 
def setup_SQLtestengine() -> sqlalchemy.engine:
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
