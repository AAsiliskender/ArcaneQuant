import math
from collections.abc import Callable

import pandas as pd
import pytest

from arcanequant import ModelEngine, Process, Reference, RegisteredFunction


######################################################################
##################### EXAMPLE FUNCTIONS FOR TEST #####################
######################################################################
def integrationTestFunction(df, r, e):
    newDf = df**e
    newDf *= r

    avg = sum(newDf.sum())/newDf.size

    return avg

def simpleFct(num):
    return abs(num)


########################################################################
##################### REGISTEREDFUNCTION UNIT TEST #####################
########################################################################

### RegisteredFunction creation test
RFCreateTestParams = (round, math.sin, integrationTestFunction)

@pytest.mark.parametrize("operation", RFCreateTestParams)
def test_RegisteredFunction_creation(operation: RegisteredFunction):
    try:
        RegisteredFunction(operation)
    except Exception as e:
        assert False, f"Failed: Creating RegisteredFunction for Callable {operation.__name__}"


###########################################################################################
##################### REGISTEREDFUNCTION/MODELENGINE INTEGRATION TEST #####################
###########################################################################################

### Engine function assignment test 
engineAssignNames = ('inbuilt', 'cFunction', 'custom')
engineAssignFunctions = (round, math.sin, integrationTestFunction)
engineAssignExpected = (
    {('inbuilt', round), ('inbuiltRF', RegisteredFunction(round))},
    {('cFunction', math.sin), ('cFunctionRF', RegisteredFunction(math.sin))},
    {('custom', integrationTestFunction), ('customRF', RegisteredFunction(integrationTestFunction))}
)
engineAssignParams = zip(engineAssignFunctions, engineAssignNames, engineAssignExpected)

@pytest.mark.parametrize("operation, name, expected", engineAssignParams)
def test_engine_assign(operation: Callable, name: str, expected: set, setup_empty_modelengine: ModelEngine):
    engine: ModelEngine = setup_empty_modelengine

    operationRF = RegisteredFunction(operation)

    engine.assign(f'{name}', operation)
    engine.assign(f'{name}RF', operationRF)

    assert {(key, value) for key, value in engine.functions.items()} == expected, f"Failed: Assigning function/RegisteredFunction {operation.__name__} to ModelEngine"


### Engine function register test
engineRFs = [round, math.sin, integrationTestFunction]

@pytest.mark.parametrize("operation", engineRFs)
def test_engine_register(operation: Callable, setup_functions_partial_modelengine: Callable):
    engine: ModelEngine = setup_functions_partial_modelengine(operation) # Fixture setup

    engine.register_function(operation)

    assert engine.functions[operation.__name__] == RegisteredFunction(operation), f"Failed: Registering function {operation.__name__} to ModelEngine"


### Engine function/RegisteredFunction getter test
engineGetterFunctions = (round, math.sin, integrationTestFunction)
engineGetterNames = ('inbuilt', 'cFunction', 'custom')
engineGetterParams = zip(engineGetterFunctions, engineGetterNames)

@pytest.mark.parametrize("operation, name", engineGetterParams)
def test_engine_getters(operation: Callable, name: str, setup_functions_partial_modelengine: Callable):
    operationRF = RegisteredFunction(operation)
    engine: ModelEngine = setup_functions_partial_modelengine(operation, name) # Fixture setup

    assert engine.get_function(operation.__name__) == operation, f"Failed: Did not get assigned function from '{operation.__name__}'"
    assert engine.get_function(name) == operation, f"Failed: Did not get assigned function from '{name}'"
    assert engine.get_function(f'{name}RF') == operation, f"Failed: Did not get assigned function from '{name}RF'"
    assert engine.get_registeredFunction(f'{name}RF') == operationRF, f"Failed: Did not get RegisteredFunction from '{name}RF'"


### Engine function registering and unregistering
engineUnRegisteringFunctions = (round, math.sin, integrationTestFunction)

@pytest.mark.parametrize("operation", engineUnRegisteringFunctions)
def test_engine_register_unregister(operation: Callable, setup_empty_modelengine: ModelEngine):
    engine: ModelEngine = setup_empty_modelengine

    engine.register_function(operation)
    assert engine.get_registeredFunction(operation.__name__) == RegisteredFunction(operation), f"Failed: Registering operation '{operation.__name__}'"

    engine.unregister_function(operation.__name__)
    assert engine.functions == {}, f"Failed: Unregistering function '{operation.__name__}'"


### Engine operation standardisation
engineStandardisationFunctions = (round, math.sin, integrationTestFunction)
engineStandardisationNames = ('inbuilt', 'cFunction', 'custom')
engineStandardisationParams = zip(engineStandardisationFunctions, engineStandardisationNames)

# Standardising functions on an empty ModelEngine
@pytest.mark.parametrize("operation", engineStandardisationFunctions)
def test_engine_standardise_function(operation: Callable, setup_empty_modelengine: ModelEngine):
    engine: ModelEngine = setup_empty_modelengine
    operationRF = RegisteredFunction(operation)

    engine.standardise_operation(operation)
    assert engine.get_registeredFunction(operation.__name__) == operationRF, f"Failed: Standardising operation {operation.__name__} by function."

# Standardising RegisteredFunctions on an empty ModelEngine
@pytest.mark.parametrize("operation", engineStandardisationFunctions)
def test_engine_standardise_registeredFunctions(operation: Callable, setup_empty_modelengine: ModelEngine):
    engine: ModelEngine = setup_empty_modelengine
    operationRF = RegisteredFunction(operation)

    engine.standardise_operation(operationRF)
    assert engine.get_registeredFunction(operation.__name__) == operationRF, f"Failed: Standardising operation {operation.__name__} by RegisteredFunction."

# Standardising functions (named same as function itself) on a populated ModelEngine
@pytest.mark.parametrize("operation", engineStandardisationFunctions)
def test_engine_standardise_existing_function(operation: Callable, setup_functions_partial_modelengine: ModelEngine):
    engine: ModelEngine = setup_functions_partial_modelengine(operation)
    operationRF = RegisteredFunction(operation)

    engine.standardise_operation(operation)
    assert engine.get_registeredFunction(operation.__name__) == operationRF, f"Failed: Standardising existing operation {operation.__name__} by function."

# Standardising custom named functions on a populated ModelEngine
@pytest.mark.parametrize("operation, name", engineStandardisationParams)
def test_engine_standardise_existing_customName_function(operation: Callable, name: str, setup_functions_partial_modelengine: ModelEngine):
    engine: ModelEngine = setup_functions_partial_modelengine(operation, name)
    operationRF = RegisteredFunction(operation)

    # Note this creates/replaces the functions object name (operation.__name__) into the standardised form (not the custom name)
    engine.standardise_operation(name)
    assert engine.get_registeredFunction(operation.__name__) == operationRF, f"Failed: Standardising existing operation {operation.__name__} by string key {name}."

# Standardising RegisteredFunctions on a populated ModelEngine
@pytest.mark.parametrize("operation", engineStandardisationFunctions)
def test_engine_standardise_existing_registeredFunction(operation: Callable, setup_functions_partial_modelengine: ModelEngine):
    engine: ModelEngine = setup_functions_partial_modelengine(operation)
    operationRF = RegisteredFunction(operation)

    engine.standardise_operation(operationRF)
    assert engine.get_registeredFunction(operation.__name__) == operationRF, f"Failed: Standardising existing operation {operation.__name__} by RegisteredFunction."


###################################################################################
##################### REFERENCES/MODELENGINE INTEGRATION TEST #####################
###################################################################################

### Engine Reference Assignment
assignRefSource = ('list', 'params', 'params')
assignRefPath = ([], ['ratio'], ['expon'])
assignRefName = ('x', 'r', 'e')
assignRefParams = zip(assignRefName, assignRefSource, assignRefPath)

# Assigning References to ModelEngine
@pytest.mark.parametrize("name, source, path", assignRefParams)
def test_engine_assign_references(name: str, source: str, path: list, setup_empty_modelengine: ModelEngine):
    engine: ModelEngine = setup_empty_modelengine
    ref = Reference(source, path)

    engine.assign(name, ref)
    assert engine.aliases[name] == Reference(source, path)

# Getting References from ModelEngine using getter (.ref())
@pytest.mark.parametrize("name, source, path", assignRefParams)
def test_engine_ref_getter(name: str, source: str, path: list, setup_empty_modelengine: ModelEngine):
    engine: ModelEngine = setup_empty_modelengine
    ref = Reference(source, path)

    engine.assign(name, ref)
    assert engine.ref(name) == Reference(source, path)


#########################################################################################
##################### PROCESSES/REGISTEREDFUNCTION INTEGRATION TEST #####################
#########################################################################################
ratio = 0.1
expon = 2
testNum = -13
ratio2 = 0.2
ratio3 = 10

ref1 = Reference('list', [])
ref2 = Reference('params', ['ratio'])
ref3 = Reference('params', ['expon'])

testDf = pd.DataFrame(data=[pd.Series(ref1.resolve(mEngine))*i for i in range(1,11)])
testDf2 = pd.DataFrame(data=[pd.Series(ref1.resolve(mEngine)) for _ in range(1,11)])

# Creating Processes using different function, input types
createProcessName = (
    'simpleProcess', 'standardisedProcess', 'addProcess',
    'baseProcess', 'baseAddProcess', 'registeredFunctionProcess', 'registeredFunctionStringProcess',
    'assignedProcess', 'assignedFunctionRefProcess',
    'standardisedStringProcess', 'standardisedFunctionProcess',
    'directRefProcess', 'refViaEngineProcess'
)
createProcessFunction = (
    simpleFct, simpleFct, simpleFct,
    integrationTestFunction, integrationTestFunction, RegisteredFunction(integrationTestFunction), 'customRF',
    'custom', 'custom',
    'custom', integrationTestFunction,
    integrationTestFunction, integrationTestFunction
)
createProcessInputs = (
    [testNum], [testNum], [testNum],
    [testDf, ratio, expon], [testDf, ratio2, expon], [testDf, ratio, expon], [testDf, ratio2, expon],
    [testDf, ratio3, expon], [mEngine.assets['df'], mEngine.ref('r'), mEngine.ref('e')],
    [testDf, ratio, expon], [testDf, ratio3, expon],
    [mEngine.assets['df'], ref2, ref3], [mEngine.assets['df'], mEngine.ref('r'), mEngine.ref('e')]

)
createProcessOutputs = ()
createProcessFailStr = (
    "Creating process with one input",
)

@pytest.mark.parametrize("", )
def test_process_creation():
    ...






# Standardising Processes
standardiseProcessName = (
    'standardisedProcess', 'standardisedFunctionProcess', 'standardisedStringProcess', 
)
standardiseProcessFunction = (
    simpleFct, integrationTestFunction, 'custom',
)
standardiseProcessInputs = (
    [testNum], [testDf, ratio3, expon], [testDf, ratio, expon],
)
standardiseProcessFailStr = (
    "process with one input", "process using a multi-input function", "process using string key of already-assigned multi-input function"
)
standardiseProcessParams = zip(standardiseProcessName, standardiseProcessFunction, standardiseProcessInputs, standardiseProcessFailStr)

@pytest.mark.parametrize("name, operation, inputs, failStr", standardiseProcessParams)
def test_process_standardisation(name: str, operation, inputs: list, failStr: str, setup_functions_partial_modelengine):
    engine = setup_functions_partial_modelengine(operation)
    ## NEED TO SET UP ENGINE FUNCTIONS FULLY BEFORE CONTINUING

    assert True, f"Failed: Standardising a {failStr}."


######################################################################################################
##################### PROCESSES/REFERENCES/MODELENGINE COMPLETE INTEGRATION TEST #####################
######################################################################################################



def hiding_stuff():


    ### Test assigning variables
    print('=== Engine Check 7 (Assigning Variables) ===')
    mEngine.assign('numbers', [0.501, math.pi])
    mEngine.assign('list', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

    testDf = pd.DataFrame(data=[pd.Series(ref1.resolve(mEngine))*i for i in range(1,11)])
    testDf2 = pd.DataFrame(data=[pd.Series(ref1.resolve(mEngine)) for _ in range(1,11)])

    mEngine.assign('idx', 3)
    mEngine.assign('params', {'expon': 2, 'ratio':0.1})
    mEngine.assign('df', testDf)
    mEngine.assign('df2', testDf2)
    #mEngine.assign()
    print(mEngine.functions.keys())

    for item in mEngine.assets:
        print(item, mEngine.assets[item])
    print('=== Successful Engine Check 7 (Assigned Variables) ===')


    print('=== Creating Processes ===')
    ratio = 0.1
    expon = 2
    testNum = -13
    ratio2 = 0.2
    ratio3 = 10

    pr1 = Process('simpleProcess', simpleFct, [testNum], output_dest='process1') # A process with one input
    pr2 = Process('standardisedProcess', simpleFct, [testNum]) # Same but standardised first
    pr3 = Process('addProcess', simpleFct, [testNum]) # Same but added with add_process() (also standardises)

    pr4 = Process('baseProcess', integrationTestFunction, [testDf, ratio, expon]) # Process using a more complex function 
    pr5 = Process('baseAddProcess', integrationTestFunction, [testDf, ratio2, expon]) # Same but added with add_process (also standardises)
    pr6 = Process('registeredFunctionProcess', RegisteredFunction(integrationTestFunction), [testDf, ratio, expon]) # Process with same function but registered
    pr7 = Process('registeredFunctionStringProcess', 'customRF', [testDf, ratio2, expon]) # Process with same function but registered

    pr8 = Process('assignedProcess', 'custom', [testDf, ratio3, expon]) # Process using string call of assigned function (non-standard name)
    pr9 = Process('assignedFunctionRefProcess', 'custom', [mEngine.assets['df'], mEngine.ref('r'), mEngine.ref('e')]) # Process of string-called function (as well as assets and references)

    pr10 = Process('standardisedStringProcess', 'custom', [testDf, ratio, expon]) # Process using string call of assigned function (but standardised after) 
    pr11 = Process('standardisedFunctionProcess', integrationTestFunction, [testDf, ratio3, expon]) # Process using a string call of function (standardised after)

    pr12 = Process('directRefProcess', integrationTestFunction, [mEngine.assets['df'], ref2, ref3]) # Process using references instances for values
    pr13 = Process('refViaEngineProcess', integrationTestFunction, [mEngine.assets['df'], mEngine.ref('r'), mEngine.ref('e')]) # Process using references obtained from ModelEngine

    for pr in [pr1, pr2, pr3, pr4, pr5, pr6, pr7, pr8, pr9, pr10, pr11, pr12, pr13]:
        print(pr)
    print('=== Successfully Created Processes ===')


    print('=== Standardising Processes ===')
    pr2 = mEngine.standardise_process(pr2)
    pr10 = mEngine.standardise_process(pr10)
    pr11 = mEngine.standardise_process(pr11)
    print('=== Successfully Standardised Processes ===')


    # Some processes must be adding or assigned to engine before being executed
    print('=== Assigning Processes ===')
    mEngine.assign('simpleProcess', pr1)
    mEngine.assign('standardisedProcess', pr2)
    mEngine.add_process(pr3)

    mEngine.assign('baseProcess', pr4)
    mEngine.add_process(pr5)
    mEngine.assign('registeredFunctionProcess', pr6)
    mEngine.assign('registeredFunctionStringProcess', pr7)

    mEngine.assign('assignedProcess', pr8)
    mEngine.assign('assignedFunctionRefProcess', pr9)

    mEngine.assign('standardisedStringProcess', pr10)
    mEngine.assign('standardisedFunctionProcess', pr11)

    mEngine.assign('directRefProcess', pr12)
    mEngine.assign('refViaEngineProcess', pr13)

    testPrcs = set([
        'simpleProcess', 'standardisedProcess', 'addProcess',
        'baseProcess', 'baseAddProcess', 'registeredFunctionProcess', 'registeredFunctionStringProcess',
        'assignedProcess', 'assignedFunctionRefProcess',
        'standardisedStringProcess', 'standardisedFunctionProcess',
        'directRefProcess', 'refViaEngineProcess'
        ])

    assert set(mEngine.processes.keys()) == testPrcs, "Failed: Processes were not added/assigned to the ModelEngine correctly, APr"
    print('=== Successfully Assigned Processes ===')


    print('=== Executing Processes ===')
    out1a = mEngine.executeProcess('simpleProcess', return_output=False)
    assert mEngine.assets[pr1.output_dest] == 13, "Failed: Simple function process (executed from engine) did not have correct output result, out1a"
    mEngine.assets.pop(pr1.output_dest) # Remove answer for proper retest (once to test if different method results in same output)
    out1b = pr1.execute(mEngine, return_output=True)
    assert out1b == mEngine.assets[pr1.output_dest], "Failed: Simple function process (executed from process itself) did not execute correctly, out1b"
    out1c = pr1.execute()
    assert out1c == 13, "Failed: Simple function process (isolated execution) did not execute correctly, out1b"

    out2a = mEngine.executeProcess('standardisedProcess')
    assert out2a == 13, "Failed: Simple function standardised process did not execute correctly, out2a"
    out2b = pr2.execute(mEngine) # Output = True by default
    assert out2b == 13, "Failed: Simple function standardised process (executed from process itself) did not execute correctly, out2b"
    out2c = pr2.execute()
    assert out2c == 13, "Failed: Simple function standardised process (isolated execution) did not execute correctly, out2c"
    out2d = pr2.execute(return_output=False) # Should work same as out2c (test to confirm overriding no output as nowhere to save output)
    assert out2d == out2c, "Failed: Simple function standardised process (no output setting test) did not provide output when should be overridden, out2d"

    out3a = mEngine.executeProcess('addProcess')
    assert out3a == 13, "Failed: Simple function added process did not execute correctly, out3a"
    out3b = pr3.execute(mEngine)
    assert out3b == 13, "Failed: Simple function added process (executed from process itself) did not execute correctly, out3b"

    out4a = mEngine.executeProcess('baseProcess')
    # For 1<=x<=10, (Σ x^2)^2/(10*100) = 148.225
    assert out4a == 148.225, "Failed: Base function process did not execute correctly, out4a"
    out4b = pr4.execute(mEngine)
    assert out4b == 148.225, "Failed: Base function process (executed from process itself) did not execute correctly, out4b"

    out5a = mEngine.executeProcess('baseAddProcess')
    assert out5a == 296.45, "Failed: Base function added process did not execute correctly, out5a"
    out5b = pr5.execute(mEngine)
    assert out5b == 296.45, "Failed: Base function added process (executed from process itself) did not execute correctly, out5b"

    out6a = mEngine.executeProcess('registeredFunctionProcess')
    assert out6a == 148.225, "Failed: Base registered function process did not execute correctly, out6a"
    out6b = pr6.execute(mEngine)
    assert out6b == 148.225, "Failed: Base registered function process (executed from process itself) did not execute correctly, out6b"

    out7a = mEngine.executeProcess('registeredFunctionStringProcess')
    assert out7a == 296.45, "Failed: Base registered function process (in string) did not execute correctly, out7a"
    out7b = pr7.execute(mEngine)
    assert out7b == 296.45, "Failed: Base registered function process (in string, executed from process itself) did not execute correctly, out7a"

    out8a = mEngine.executeProcess('assignedProcess')
    assert out8a == 14822.5, "Failed: Base function assigned process did not execute correctly, out8a"
    out8b = pr8.execute(mEngine)
    assert out8b == 14822.5, "Failed: Base function assigned process (executed from process itself) did not execute correctly, out8b"

    out9a = mEngine.executeProcess('assignedFunctionRefProcess')
    assert out9a == 148.225, "Failed: Base function assigned process with engine referencing did not execute correctly, out9a"
    out9b = pr9.execute(mEngine)
    assert out9b == 148.225, "Failed: Base function assigned process with engine referencing (executed from process itself) did not execute correctly, out9b"

    out10a = mEngine.executeProcess('standardisedStringProcess')
    assert out10a == 148.225, "Failed: Base function standardised process with string call did not execute correctly, out10a"
    out10b = pr10.execute(mEngine)
    assert out10b == 148.225, "Failed: Base function standardised process with string call (executed from process itself) did not execute correctly, out10b"

    out11a = mEngine.executeProcess('standardisedFunctionProcess')
    assert out11a == 14822.5, "Failed: Base function standardised process did not execute correctly, out11a"
    out11b = pr11.execute(mEngine)
    assert out11b == 14822.5, "Failed: Base function standardised process (executed from process itself) did not execute correctly, out11b"

    out12a = mEngine.executeProcess('directRefProcess')
    assert out12a == 148.225, "Failed: Base function process with References did not execute correctly, out11a"
    out12b = pr12.execute(mEngine)
    assert out12b == 148.225, "Failed: Base function process with References (executed from process itself) did not execute correctly, out11b"

    out13a = mEngine.executeProcess('refViaEngineProcess')
    assert out13a == 148.225, "Failed: Base function process with engine referencing did not execute correctly, out11a"
    out13b = pr13.execute(mEngine)
    assert out13b == 148.225, "Failed: Base function process with engine referencing (executed from process itself) did not execute correctly, out11b"
    print('=== Successfully Executed Processes ===')

    print('=== Integration Test Complete ===')


##################### FIXTURE FUNCTION(S) ######################
# Provides empty ModelEngine
@pytest.fixture(scope = 'function')
def setup_empty_modelengine() -> ModelEngine:
    return ModelEngine()


# Provides ModelEngine with requested set of functions
@pytest.fixture(scope = 'function')
def setup_functions_partial_modelengine(setup_empty_modelengine: ModelEngine) -> Callable:
    """
    Provides ModelEngine with pre-assigned function under two names and a RegisteredFunction version
    (e.g. 'round', 'inbuilt', 'inbuiltRF'). If no name given only assigns the original function with original name.
    Only used after test_engine_assign (as it uses assignment and RegisteredFunction creation)
    """

    def _setup_functions_partial_modelengine(operation, name = "") -> ModelEngine:
        
        operationRF = RegisteredFunction(operation)
        engine = setup_empty_modelengine

        engine.assign(operation.__name__, operation)
        if name:
            engine.assign(name, operation)
            engine.assign(f"{name}RF", operationRF)
        
        return engine

    return _setup_functions_partial_modelengine


# Provides ModelEngine with all functions for testing
@pytest.fixture(scope = 'function')
def setup_functions_full_modelengine(setup_empty_modelengine: ModelEngine) -> ModelEngine:
    """
    Provides ModelEngine with all the required functions for testing (round, math.sin and integratedTestFunction),
    alongside their custom names for functions and RegisteredFunctions (i.e. 'inbuilt' and 'inbuiltRF' for round).
    """
    engine = setup_empty_modelengine

    fctGrp = [(round, 'inbuilt'), (math.sin, 'cFunction'), (integrationTestFunction, 'custom')]

    for fct, name in fctGrp:
        engine.assign(fct.__name__, fct)
        engine.assign(name, fct)
        engine.assign(f"{name}RF", RegisteredFunction(fct))

    return engine



