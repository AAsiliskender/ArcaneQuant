import math
from collections.abc import Callable

import pandas as pd
import pytest

from arcanequant import ModelEngine, Process, Reference, RegisteredFunction

# Note that each test case counts as one test no matter the number of assertions

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
engineAssignParams = list(zip(engineAssignFunctions, engineAssignNames, engineAssignExpected))

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
engineGetterParams = list(zip(engineGetterFunctions, engineGetterNames))

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
engineStandardisationParams = list(zip(engineStandardisationFunctions, engineStandardisationNames))

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
assignRefParams = list(zip(assignRefName, assignRefSource, assignRefPath))

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


######################################################################
################### PROCESS TEST SETUP AND HELPERS ###################
######################################################################

ratio = 0.1
expon = 2
testNum = -13
ratio2 = 0.2
ratio3 = 10

ref1 = Reference('list', [])
ref2 = Reference('params', ['ratio'])
ref3 = Reference('params', ['expon'])

def setup_process_modelengine() -> ModelEngine:
    """Build a populated engine directly, including inputs for process test cases."""
    engine = ModelEngine()
    for operation, name in ((round, 'inbuilt'), (math.sin, 'cFunction'), (integrationTestFunction, 'custom')):
        engine.assign(operation.__name__, operation)
        engine.assign(name, operation)
        engine.assign(f'{name}RF', RegisteredFunction(operation))

    engine.assign('numbers', [0.501, math.pi])
    engine.assign('list', list(range(1, 11)))
    engine.assign('idx', 3)
    engine.assign('params', {'expon': expon, 'ratio': ratio})
    engine.assign('x', ref1)
    engine.assign('r', ref2)
    engine.assign('e', ref3)
    values = pd.Series(ref1.resolve(engine))
    engine.assign('df', pd.DataFrame([values * i for i in range(1, 11)]))
    engine.assign('df2', pd.DataFrame([values for _ in range(10)]))
    return engine


mEngine = setup_process_modelengine()
testDf = mEngine.assets['df']
testDf2 = mEngine.assets['df2']

### Creating, assigning and executing processes using different operation and input types
createProcessName = (
    'simpleProcess', 'standardisedProcess', 'addProcess',
    'baseProcess', 'baseAddProcess', 'registeredFunctionProcess', 'registeredFunctionStringProcess',
    'assignedProcess', 'assignedFunctionRefProcess',
    'standardisedStringProcess', 'standardisedFunctionProcess',
    'directRefProcess', 'refViaEngineProcess',
)
createProcessFunction = (
    simpleFct, simpleFct, simpleFct,
    integrationTestFunction, integrationTestFunction, RegisteredFunction(integrationTestFunction), 'customRF',
    'custom', 'custom',
    'custom', integrationTestFunction,
    integrationTestFunction, integrationTestFunction,
)
createProcessInputs = (
    [testNum], [testNum], [testNum],
    [testDf, ratio, expon], [testDf, ratio2, expon], [testDf, ratio, expon], [testDf, ratio2, expon],
    [testDf, ratio3, expon], [mEngine.assets['df'], mEngine.ref('r'), mEngine.ref('e')],
    [testDf, ratio, expon], [testDf, ratio3, expon],
    [mEngine.assets['df'], ref2, ref3], [mEngine.assets['df'], mEngine.ref('r'), mEngine.ref('e')],
)
createProcessOutputs = (
    13, 13, 13,
    148.225, 296.45, 148.225, 296.45,
    14822.5, 148.225,
    148.225, 14822.5,
    148.225, 148.225
)

createProcessMethod = (
    'assign', 'standardise', 'add',
    'assign', 'add', 'assign', 'assign',
    'assign', 'assign', 'standardise', 'standardise', 'assign', 'assign',
)
createProcessParams = list(zip(
    (mEngine,) * len(createProcessName), createProcessName, createProcessFunction,
    createProcessInputs, createProcessOutputs, createProcessMethod,
))

def check_process_creation(engine: ModelEngine, name: str, operation, inputs: list, expected: float, method: str):
    # Keep registry changes local so each case can also run independently.
    engine = ModelEngine(assets=engine.assets.copy(), aliases=engine.aliases.copy(), functions=engine.functions.copy())
    process = Process(name, operation, inputs)
    assert process.name == name
    assert process.operation == operation
    assert process.input_args is inputs

    if method == 'standardise':
        assert engine.standardise_process(process) is process
        engine.assign(name, process)
    elif method == 'add':
        engine.add_process(process, standardise=True)
    else:
        engine.assign(name, process)

    assert engine.processes == {name: process}, f"Failed: Assigning process '{name}'."
    if method in ('standardise', 'add'):
        assert isinstance(process.operation, RegisteredFunction), f"Failed: Standardising process '{name}'."
    assert engine.executeProcess(name) == pytest.approx(expected), f"Failed: Executing process '{name}' from engine."
    assert process.execute(engine) == pytest.approx(expected), f"Failed: Executing process '{name}' directly."

# Split cases by whether their input arguments (case[3]) contain any Reference objects.
processFunctionParams = [case for case in createProcessParams if not any(isinstance(value, Reference) for value in case[3])]
processReferenceParams = [case for case in createProcessParams if any(isinstance(value, Reference) for value in case[3])]


######################################################################
##################### MODELENGINE ASSET UNIT TEST ####################
######################################################################

### Engine asset assignment
processAssetNames = ('numbers', 'list', 'idx', 'params', 'df', 'df2')
processAssetExpected = (
    [0.501, math.pi], list(range(1, 11)), 3, {'expon': expon, 'ratio': ratio},
    pd.DataFrame([[i * j for j in range(1, 11)] for i in range(1, 11)]),
    pd.DataFrame([list(range(1, 11)) for _ in range(10)]),
)
processAssetParams = list(zip((mEngine,) * len(processAssetNames), processAssetNames, processAssetExpected))

@pytest.mark.parametrize("engine, name, expected", processAssetParams)
def test_process_engine_assets(engine: ModelEngine, name: str, expected):
    assert name in engine.assets, f"Failed: Assigning process input asset '{name}'."
    if isinstance(expected, pd.DataFrame):
        pd.testing.assert_frame_equal(engine.assets[name], expected)
    else:
        assert engine.assets[name] == expected, f"Failed: Assigning process input asset '{name}'."


###########################################################################################
###################### PROCESSES/REGISTEREDFUNCTION INTEGRATION TEST ######################
###########################################################################################

@pytest.mark.parametrize("engine, name, operation, inputs, expected, method", processFunctionParams)
def test_process_creation(engine: ModelEngine, name: str, operation, inputs: list, expected: float, method: str):
    check_process_creation(engine, name, operation, inputs, expected, method)


### Executing standalone processes
standaloneProcessOperations = (simpleFct, RegisteredFunction(simpleFct))
standaloneProcessInputs = ([testNum], [testNum])
standaloneProcessOutputs = (13, 13)
standaloneProcessParams = list(zip(standaloneProcessOperations, standaloneProcessInputs, standaloneProcessOutputs))

@pytest.mark.parametrize("operation, inputs, expected", standaloneProcessParams)
def test_process_standalone_execution(operation, inputs: list, expected: float):
    process = Process('standaloneProcess', operation, inputs)
    assert process.execute() == expected
    assert process.execute(return_output=False) == expected


###########################################################################################
############### PROCESSES/REFERENCES/MODELENGINE COMPLETE INTEGRATION TEST ################
###########################################################################################

@pytest.mark.parametrize("engine, name, operation, inputs, expected, method", processReferenceParams)
def test_process_reference_execution(engine: ModelEngine, name: str, operation, inputs: list, expected: float, method: str):
    check_process_creation(engine, name, operation, inputs, expected, method)


### Executing processes and saving outputs in the engine
processOutputReturn = (True, False)
processOutputExpected = (13, None)
processOutputParams = list(zip((mEngine,) * 2, processOutputReturn, processOutputExpected))

@pytest.mark.parametrize("engine, return_output, expected", processOutputParams)
def test_process_output_destination(engine: ModelEngine, return_output: bool, expected):
    engine = ModelEngine(assets=engine.assets.copy())
    process = Process('simpleProcess', simpleFct, [testNum], output_dest='process1')
    engine.assign(process.name, process)
    assert engine.executeProcess(process.name, return_output=return_output) == expected
    assert engine.assets.pop('process1') == 13
    assert process.execute(engine, return_output=return_output) == expected
    assert engine.assets['process1'] == 13


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



