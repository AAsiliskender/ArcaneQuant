# Integraton Testing of SQLExecute function with database (direct query) and pandas (read_sql_query function)
# Tests if a generic input provided, results in the expected changes as per input.
# The result is read by using SQLAlchemy connect_engine provided connection, and separately using pandas

# TO DO TO COMPLETE THIS TEST:
# GENERATE A TEMPORARY DATABASE THAT DISAPPEARS AFTER SESSION OVER
# IMPORT CREATE ENGINE OR SOMETHING LIKE THAT

# Packages
import pytest
import pandas as pd
from datetime import time as dt_time
from tabulate import tabulate
from sqlalchemy import text

# My packages
from arcanequant.quantlib.DataManifestManager import DataManifest
from arcanequant.quantlib.SQLManager import ExecuteSQL

testManifest = DataManifest()
testManifest.connectSQL('testSQLlogin')


# need sqlalchemy (and docker imports, after learning to set up docker)

testquery = f"""
                    DROP TABLE IF EXISTS \"testTable\" CASCADE;
    
                    CREATE TABLE \"testTable\" (
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

                    INSERT INTO \"testTable\" (\"Time\")
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


testParams = [('Pandas', pd_expected), ('Direct', sql_expected)]

@pytest.mark.parametrize("testFrom,expected", testParams)
def test_ExecuteSQL(testFrom, expected):

    ExecuteSQL(testquery, testManifest.SQLengine)

    # Extract what the table should look like and compare to what it should be
    # For example, from SQL its a string of the table appearance and contents
    # From Pandas it is the values within the table being checked
    with testManifest.SQLengine.connect() as conn:
        if testFrom == 'Pandas':
            result = pd.read_sql_query("SELECT * FROM \"testTable\";", conn)
            
            teardownquery = conn.execute(text("DROP TABLE IF EXISTS \"testTable\" CASCADE;"))
            conn.commit()
            
            assert result.values.tolist() == expected
            
        
        elif testFrom == 'Direct':
            extract = conn.execute(text("SELECT * FROM \"testTable\";"))

            rows = extract.fetchall()  # Fetch all rows
            headers = extract.keys() # Fetch all headers

            result = tabulate(rows, headers=headers, tablefmt="psql")
        
            teardownquery = conn.execute(text("DROP TABLE IF EXISTS \"testTable\" CASCADE;"))
            conn.commit()

            assert result == expected

