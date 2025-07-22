# Packages
import pytest

# My packages
from arcanequant.quantlib.DataManifestManager import DataManifest
from arcanequant.quantlib.SQLManager import SetKeysQuery, DropKeysQuery

################################################################
#####################  SET KEYS QUERY TEST  ####################
################################################################

################ TEST QUERY AND EXPECTED RESULTS ###############
### Primary key
groundPS1 = """
                        ALTER TABLE "testTable1"
                        DROP CONSTRAINT IF EXISTS "testTable1_pkey" CASCADE;

                        ALTER TABLE "testTable1"
                        ADD PRIMARY KEY ("BigInt");
                        """
testPS1 = ('testTable1', 'BigInt', 'primary', None, groundPS1)

### Composite (primary) key
groundCS1 = """
                        ALTER TABLE "testTable2"
                        DROP CONSTRAINT IF EXISTS "testTable2_pkey" CASCADE;

                        ALTER TABLE "testTable2"
                        ADD PRIMARY KEY ("SmallInt", "Float2");
                        """
testCS1 = ('testTable2', ['SmallInt', 'Float2'], 'primary', None, groundCS1)

### Unique
groundUS1 = """
                        ALTER TABLE "testTable2"
                        DROP CONSTRAINT IF EXISTS "unique_testTable2_SmallInt" CASCADE,
                        ADD CONSTRAINT "unique_testTable2_SmallInt" UNIQUE ("SmallInt");
                        """
testUS1 = ('testTable2', 'SmallInt', 'unique', None, groundUS1)

### Composite unique key
groundCUS1 = """
                            ALTER TABLE "testTable2"
                            DROP CONSTRAINT IF EXISTS "unique_testTable2_BigInt-Time" CASCADE,
                            ADD CONSTRAINT "unique_testTable2_BigInt-Time" UNIQUE ("BigInt", "Time");
                            """
testCUS1 = ('testTable2', ['BigInt', 'Time'], 'unique', None, groundCUS1)

### Single non-composite foreign
groundSFS1 = """
                        ALTER TABLE "testTable1"
                        DROP CONSTRAINT IF EXISTS "fk_testTable1_BigInt_testTable3_BigInt2" CASCADE,
                        ADD CONSTRAINT "fk_testTable1_BigInt_testTable3_BigInt2"
                        FOREIGN KEY ("BigInt") REFERENCES "testTable3"("BigInt2") ON UPDATE CASCADE;
                        """
testSFS1 = ('testTable1', 'BigInt', 'foreign', ('testTable3','BigInt2'), groundSFS1)

### Single composite foreign
groundSCFS1 = """
                            ALTER TABLE "testTable2"
                            DROP CONSTRAINT IF EXISTS "fk_testTable2_BigInt-Time_testTable3_BigInt2-Time2" CASCADE,
                            ADD CONSTRAINT "fk_testTable2_BigInt-Time_testTable3_BigInt2-Time2"
                            FOREIGN KEY ("BigInt", "Time") REFERENCES "testTable3" ("BigInt2", "Time2") ON UPDATE CASCADE;
                            """
testSCFS1 = ('testTable2', ['BigInt', 'Time'], 'foreign', ('testTable3',('BigInt2','Time2')), groundSCFS1)

### Multiple non-composite foreign
groundMFS1 = """
                                ALTER TABLE "testTable1"
                                DROP CONSTRAINT IF EXISTS "fk_testTable1_SmallInt_testTable2_SmallInt2" CASCADE,
                                ADD CONSTRAINT "fk_testTable1_SmallInt_testTable2_SmallInt2"
                                FOREIGN KEY ("SmallInt") REFERENCES "testTable2"("SmallInt2") ON UPDATE CASCADE;
                                
                                ALTER TABLE "testTable1"
                                DROP CONSTRAINT IF EXISTS "fk_testTable1_BigInt_testTable3_BigInt2" CASCADE,
                                ADD CONSTRAINT "fk_testTable1_BigInt_testTable3_BigInt2"
                                FOREIGN KEY ("BigInt") REFERENCES "testTable3"("BigInt2") ON UPDATE CASCADE;
                                """
testMFS1 = ('testTable1', ['SmallInt', 'BigInt'], 'foreign', [('testTable2','SmallInt2'),('testTable3','BigInt2')], groundMFS1)

### Multiple composite foreign
groundMCFS1 = """
                                ALTER TABLE "testTable1"
                                DROP CONSTRAINT IF EXISTS "fk_testTable1_BigInt-Time_testTable2_BigInt2-Time2" CASCADE,
                                ADD CONSTRAINT "fk_testTable1_BigInt-Time_testTable2_BigInt2-Time2"
                                FOREIGN KEY ("BigInt", "Time") REFERENCES "testTable2" ("BigInt2", "Time2") ON UPDATE CASCADE;
                                
                                ALTER TABLE "testTable1"
                                DROP CONSTRAINT IF EXISTS "fk_testTable1_BigInt-Time_testTable3_BigInt3-Time3" CASCADE,
                                ADD CONSTRAINT "fk_testTable1_BigInt-Time_testTable3_BigInt3-Time3"
                                FOREIGN KEY ("BigInt", "Time") REFERENCES "testTable3" ("BigInt3", "Time3") ON UPDATE CASCADE;
                                """
testMCFS1 = ('testTable1', ['BigInt', 'Time'], 'foreign', [('testTable2',('BigInt2','Time2')),('testTable3',('BigInt3','Time3'))], groundMCFS1)

# Multiple non-composite using composite form foreign
groundMCFS1_NC = """
                                ALTER TABLE "testTable1"
                                DROP CONSTRAINT IF EXISTS "fk_testTable1_SmallInt_testTable2_SmallInt2" CASCADE,
                                ADD CONSTRAINT "fk_testTable1_SmallInt_testTable2_SmallInt2"
                                FOREIGN KEY ("SmallInt") REFERENCES "testTable2" ("SmallInt2") ON UPDATE CASCADE;
                                
                                ALTER TABLE "testTable1"
                                DROP CONSTRAINT IF EXISTS "fk_testTable1_SmallInt_testTable3_SmallInt3" CASCADE,
                                ADD CONSTRAINT "fk_testTable1_SmallInt_testTable3_SmallInt3"
                                FOREIGN KEY ("SmallInt") REFERENCES "testTable3" ("SmallInt3") ON UPDATE CASCADE;
                                """
testMCFS1_NC = ('testTable1', ['SmallInt'], 'foreign', [('testTable2',['SmallInt2']),('testTable3',['SmallInt3'])], groundMCFS1_NC)

### Secondary non-composite key
groundSS1 = """
                        DROP INDEX IF EXISTS "ix_testTable1_Int" CASCADE;
                        CREATE INDEX "ix_testTable1_Int" ON "testTable1" ("Int");
                        """
testSS1 = ('testTable1', 'Int', 'secondary', None, groundSS1)

### Secondary composite key
groundSCS1 = """
                        DROP INDEX IF EXISTS "ix_testTable1_Int-BigInt" CASCADE;
                        CREATE INDEX "ix_testTable1_Int-BigInt" ON "testTable1" ("Int", "BigInt");
                        """
testSCS1 = ('testTable1', ('Int','BigInt'), 'secondary', None, groundSCS1)

querySetKeysTest = [testPS1, testCS1, testUS1, testCUS1, testSFS1, testSCFS1, testMFS1, testMCFS1, testMCFS1_NC, testSS1, testSCS1]

######################### TEST FUNCTION ########################

@pytest.mark.parametrize("tableName,keys,kType,ref,expected", querySetKeysTest)
def test_SetKeysQuery_generator(tableName: str, keys, kType: str, ref, expected: str):
    """
    Unit tests the query generation in the SetKeysQuery method (primary component).
    Tests all expected input types (tableName, key, kTypes, ref).
    Does not test the engine input as that is not an isolated component (does not run queries).
    Also all inputs in each call (queryTestSet) is unique.
    """
    print('Testing SetKeysQuery Function...')
    assert SetKeysQuery(tableName,keys,kType,ref) == expected


################################################################
####################  DROP KEYS QUERY TEST  ####################
################################################################

################ TEST QUERY AND EXPECTED RESULTS ###############
### Primary key
groundPD1 = """
                        ALTER TABLE "testTable1"
                        DROP CONSTRAINT IF EXISTS "testTable1_pkey" CASCADE;
                        """
testPD1 = ('testTable1', 'BigInt', 'primary', None, groundPD1)

### Composite (primary) key
groundCD1 = """
                        ALTER TABLE "testTable2"
                        DROP CONSTRAINT IF EXISTS "testTable2_pkey" CASCADE;
                        """
testCD1 = ('testTable2', ['SmallInt', 'Float2'], 'primary', None, groundCD1)

### Unique Key
groundUD1 = """
                            ALTER TABLE "testTable2"
                            DROP CONSTRAINT IF EXISTS "unique_testTable2_SmallInt" CASCADE;
                            """
testUD1 = ('testTable2', 'SmallInt', 'unique', None, groundUD1)

### Composite unique key
groundCUD1 = """
                            ALTER TABLE "testTable2"
                            DROP CONSTRAINT IF EXISTS "unique_testTable2_BigInt-Time" CASCADE;
                            """
testCUD1 = ('testTable2', ['BigInt', 'Time'], 'unique', None, groundCUD1)

### Single non-composite foreign
groundSFD1 = """
                        ALTER TABLE "testTable1"
                        DROP CONSTRAINT IF EXISTS "fk_testTable1_BigInt_testTable3_BigInt2" CASCADE;
                        """
testSFD1 = ('testTable1', 'BigInt', 'foreign', ('testTable3','BigInt2'), groundSFD1)

### Single composite foreign
groundSCFD1 = """
                            ALTER TABLE "testTable2"
                            DROP CONSTRAINT IF EXISTS "fk_testTable2_BigInt-Time_testTable3_BigInt2-Time2" CASCADE;
                            """
testSCFD1 = ('testTable2', ['BigInt', 'Time'], 'foreign', ('testTable3',('BigInt2','Time2')), groundSCFD1)

### Multiple non-composite foreign
groundMFD1 = """
                                ALTER TABLE "testTable1"
                                DROP CONSTRAINT IF EXISTS "fk_testTable1_SmallInt_testTable2_SmallInt2" CASCADE;
                                
                                ALTER TABLE "testTable1"
                                DROP CONSTRAINT IF EXISTS "fk_testTable1_BigInt_testTable3_BigInt2" CASCADE;
                                """
testMFD1 = ('testTable1', ['SmallInt', 'BigInt'], 'foreign', [('testTable2','SmallInt2'),('testTable3','BigInt2')], groundMFD1)

### Multiple composite foreign
groundMCFD1 = """
                                ALTER TABLE "testTable1"
                                DROP CONSTRAINT IF EXISTS "fk_testTable1_BigInt-Time_testTable2_BigInt2-Time2" CASCADE;
                                
                                ALTER TABLE "testTable1"
                                DROP CONSTRAINT IF EXISTS "fk_testTable1_BigInt-Time_testTable3_BigInt3-Time3" CASCADE;
                                """
testMCFD1 = ('testTable1', ['BigInt', 'Time'], 'foreign', [('testTable2',('BigInt2','Time2')),('testTable3',('BigInt3','Time3'))], groundMCFD1)

# Multiple non-composite using composite form foreign
groundMCFD1_NC = """
                                ALTER TABLE "testTable1"
                                DROP CONSTRAINT IF EXISTS "fk_testTable1_SmallInt_testTable2_SmallInt2" CASCADE;
                                
                                ALTER TABLE "testTable1"
                                DROP CONSTRAINT IF EXISTS "fk_testTable1_SmallInt_testTable3_SmallInt3" CASCADE;
                                """
testMCFD1_NC = ('testTable1', ['SmallInt'], 'foreign', [('testTable2',['SmallInt2']),('testTable3',['SmallInt3'])], groundMCFD1_NC)

### Secondary non-composite key
groundSD1 = """
                            DROP INDEX IF EXISTS "ix_testTable1_Int" CASCADE;
                            """
testSD1 = ('testTable1', 'Int', 'secondary', None, groundSD1) 

### Secondary composite key
groundSCD1 = """
                        DROP INDEX IF EXISTS "ix_testTable1_Int-BigInt" CASCADE;
                        """
testSCD1 = ('testTable1', ('Int','BigInt'), 'secondary', None, groundSCD1)

queryDropKeysTest = [testPD1, testCD1, testUD1, testCUD1, testSFD1, testSCFD1, testMFD1, testMCFD1, testMCFD1_NC, testSD1, testSCD1]

######################### TEST FUNCTION ########################
@pytest.mark.parametrize("tableName,keys,kType,ref,expected", queryDropKeysTest)
def test_DropKeysQuery_generator(tableName: str, keys, kType: str, ref, expected: str):
    """
    Unit tests the query generation in the DropKeysQuery method (primary component).
    Tests all expected input types (tableName, key, kTypes, ref).
    Does not test the engine input as that is not an isolated component (does not run queries).
    Also all inputs in each call (queryTestSet) is unique.
    """
    print('Testing DropKeysQuery Function...')
    assert DropKeysQuery(tableName,keys,kType,ref) == expected

#########################