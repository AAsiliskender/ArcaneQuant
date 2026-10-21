################################################################
##################### REFERENCES UNIT TEST #####################
################################################################
# Tests the creation and pathing correctness of the References Class

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import pandas as pd
import pytest

from arcanequant.quantlib import Deferred, ModelEngine, Reference


# Test functions and classes
def sumFunction(a, b, c=0):
    return a + b + c

def helloFunction():
    return "Hello World"

@dataclass
class TestClass:
    x: int
    y: int
    z: int

    fct: Callable = round
    
    def __post_init__(self):
        self.volume = self.x * self.y * self.z
        self.colour = "green"
    
    def __call__(self, *args, **kwargs):
        return self.fct(*args, **kwargs)
    

# Test assets variable (test works on one source like assets, it will work in the others)
test_assets = {
    "testdf":pd.DataFrame(data=[[1,0,0,0,1,1,0],[10,-45,13,234,91,63,23],["testval","min","axs","as","night","asqweq","a"]],columns=['c1','c2','c3','c4','c5','c6','c7'], index=['aa','bb','cc']),
    "params":{'p1':0, 'p2':True, 'p3':round, 'p4':[2,65], 'p5':range(0,5), 'p6': abs, ('p1','p2'):-11},
    'fct1': round,
    'ser': pd.Series(data=[1,0,10,-20,50], index=['a','b','c','d',5]),
    'listvals': [99,-11,1000,-123],
    "tupvals": (100,200,400,800,1600),
    "bigrange": range(10, 50, 2),
    "internalReference": Reference(source="testdf", path=["c3"]), # Same as refDFCSe
    "deferredReference": Deferred(Reference(source="ser",path=[[0,2,3]])), # Same as refSeSiSe
    "keynumber": 1000,
    "textstring": "This is a test string",
    "sumFct": sumFunction,
    "helloFct": helloFunction,
    "lambdaHelloFct": lambda : "Hi world",
    "lambdaFct": lambda x, y: x**y,
    "allsets": {1,2,3,4,5,6,7,8,9},
    "green_cube": TestClass(2,3,4)
}

# Test aliases variable
test_aliases = {
}

# Test ModelEngine
eng = ModelEngine(assets=test_assets, aliases = test_aliases)

####################################################
########## Test Reference Creation Assets ##########
####################################################
# Arguments for Reference creation (also used for Test Reference Resolution)
refCreateArgs = {
    ### Reference test items using empty/none path
    'refTET': ("tupvals", []),
    'refLNL': ("listvals", None),

    ### Reference test items for pd.Series
    'refSeSiSe': ("ser", [[0,2,3]]),
    'refSeSnSe': ("ser", [['a','d',5]]),
    'refSeIV': ("ser", [0]),
    'refSeNV': ("ser", ['b']),

    ### Reference test items for pd.DataFrame
    'refDFCSe': ("testdf", ["c3"]),
    'refDFCV': ("testdf", ["c3",2]),
    'refDFCDFS': ("testdf", [['c1']]),
    'refDFCDFM': ("testdf", [['c1','c5']]),
    'refDFRnSe': ("testdf", ["bb"]),
    'refDFRiSe': ("testdf", [2]),
    'refDFRV': ("testdf", [1,3]),
    'refDFRiDFS': ("testdf", [[1]]),
    'refDFRiDFM': ("testdf", [[1,2]]),
    'refDfRnDF': ("testdf", [['aa','cc']]),
    
    ### Reference test items for dict
    'refDV': ("params", ['p2']),
    'refDTV': ("params", [('p1','p2')]),
    'refDTD': ("params", [('p1','p4', 'p3')]),
    'refDLDs': ("params", [['p1','p6','p5',('p1','p2')]]),
    'refDLDi': ("params", [['p2','p5','p99',1001]]),

    ### Reference test items for ordered Iterables (list, tuple, range)
    'refLV': ("listvals", [2]),
    'refLSL': ("listvals", [slice(1,4,1)]),
    'refTV': ("tupvals", [1]),
    'refTST': ("tupvals", [slice(2,5,1)]),
    'refRV': ("bigrange", [10]),
    'refRSR': ("bigrange", [slice(1,12,3)]),

    ### Reference test items for References
    'refRefSe': ("internalReference", [eng]),
    
    ### Reference test items for Deferreds
    'refDefRef': ("deferredReference", [None]),

    ### Reference test items for Numbers
    'refN': ("keynumber", [0,'a',2]),

    ### Reference test items for Strings
    'refStrC': ("textstring", [1]),
    'refStrS': ("textstring", [slice(5,10)]),

    ### Reference test items for Callables
    'refCallNN': ("helloFct", [None]),
    'refCallNA': ("sumFct", [((1, 5, 3), None)]),
    'refCallNM': ("sumFct", [((1, 5), {'c': 7})]),
    'refCallNK': ("sumFct", [(None, {'a': 2, 'b': 3, 'c': 6})]),

    'refCallLN': ("lambdaHelloFct", [None]),
    'refCallLA': ("lambdaFct", [((2, 3), None)]),
    'refCallLK': ("lambdaFct", [(None, {'x': 4, 'y': 3})]),
    'refCallLM': ("lambdaFct", [((3,), {'y': 4})]),

    ### Reference test items for Sets
    'refSSO': ("allsets", [{1, 2, 3, 4, 5, 6, 7, 8, 9}]),
    'refSLO': ("allsets", [[1, 2, 3, 4, 5, 6, 7, 8, 9]]),
    'refSTO': ("allsets", [(1, 2, 3, 4, 5, 6, 7, 8, 9)]),

    'refSSP': ("allsets", [{1, 3, 5, 7, 9}]),
    'refSLP': ("allsets", [[1, 3, 5, 7, 9]]),
    'refSTP': ("allsets", [(1, 3, 5, 7, 9)]),

    'refSSE': ("allsets", [{}]),
    'refSLE': ("allsets", [[]]),
    'refSTE': ("allsets", [()]),

    'refSSI': ("allsets", [{1, 3, 9, 27, 81}]),
    'refSLI': ("allsets", [[1, 3, 9, 27, 81]]),
    'refSTI': ("allsets", [(1, 3, 9, 27, 81)]),

    ### Reference test items for Attributes
    'refCAV': ("green_cube", ['volume']),
    'refCAC': ("green_cube", ['colour']),
    'refCN': ("green_cube", ["doesThisAttributeExist?"])
}

# Fail strings for Reference creation test
refCreateFailStr = {
    ### Reference test items using empty/none path
    'refTET' : "Using a Empty Path Iterable",
    'refLNL' : "Using a None Path",

    ### Reference test items for pd.Series
    'refSeSiSe': "Taking pd.Series from pd.Series via Slice of index location",
    'refSeSnSe': "Taking pd.Series from pd.Series via Slice of index name",
    'refSeIV': "Taking Value from pd.Series via index location",
    'refSeNV': "Taking Value from pd.Series via index name",

    ### Reference test items for pd.DataFrame
    'refDFCSe': "Taking pd.Series from pd.DataFrame via Column",
    'refDFCV': "Taking Value from pd.DataFrame via Column",
    'refDFCDFS': "Taking (subset) pd.DataFrame from pd.DataFrame via Column (Single column)",
    'refDFCDFM': "Taking (subset) pd.DataFrame from pd.DataFrame via Column (Multiple columns)",
    'refDFRnSe': "Taking pd.Series from pd.DataFrame via Row name",
    'refDFRiSe': "Taking pd.Series from pd.DataFrame via Row index",
    'refDFRV': "Taking Value from pd.DataFrame via Row (then column)",
    'refDFRiDFS': "Taking (subset) pd.Dataframe from pd.DataFrame via Row index (Single row)",
    'refDFRiDFM': "Taking (subset) pd.Dataframe from pd.DataFrame via Row index, (Multiple rows)",
    'refDfRnDF': "Taking (subset) pd.Dataframe from pd.DataFrame via Row name",

    ### Reference test items for dict
    'refDV': "Taking Value from dictionary",
    'refDTV': "Taking Value from dictionary via tuple",
    'refDTD': "Taking (subset) dictionary from dictionary via tuple",
    'refDLDs': "Taking (subset) dictionary from dictionary via list",
    'refDLDi': "Taking (intersection) dictionary from dictionary via list",

    ### Reference test items for ordered Iterables (list, tuple, range)
    'refLV': "Taking Value from List",
    'refLSL': "Taking List from List via Slice",
    'refTV': "Taking Value from Tuple",
    'refTST': "Taking Tuple from Tuple via Slice",
    'refRV': "Taking Value from Range",
    'refRSR': "Taking Range from Range via Slice",

    ### Reference test items for References
    'refRefSe': "Taking referenced Series from Reference via resolve",

    ### Reference test items for Deferreds
    'refDefRef': "Taking deferred Reference from Deferred via resolve",

    ### Reference test items for Numbers
    'refN': "Taking number and then trying to access further path (should warn, in other settings can raise error)",

    ### Reference test items for Strings
    'refStrC': "Taking single character from string via index",
    'refStrS': "Taking substring from string via slice",

    ### Reference test items for Callables
    'refCallNN': "Calling a (named) function with no arguments",
    'refCallNA': "Calling a (named) function with argument types (Positional arguments only)",
    'refCallNK': "Calling a (named) function with argument types (Keyword arguments only)",
    'refCallNM': "Calling a (named) function with argument types (Mixed positional and keyword arguments)",

    'refCallLN': "Calling a lambda function with no arguments",
    'refCallLA': "Calling a lambda function with argument types (Positional arguments only)",
    'refCallLK': "Calling a lambda function with argument types (Keyword arguments only)",
    'refCallLM': "Calling a lambda function with argument types (Mixed positional and keyword arguments)",

    ### Reference test items for Sets
    'refSSO': "Taking (back original) set from set via input types (Set input)",
    'refSLO': "Taking (back original) set from set via input types (List input)",
    'refSTO': "Taking (back original) set from set via input types (Tuple input)",

    'refSSP': "Taking partial set from set via input types (Set input)",
    'refSLP': "Taking partial set from set via input types (List input)",
    'refSTP': "Taking partial set from set via input types (Tuple input)",

    'refSSE': "Taking empty set from set via input types (Set input)",
    'refSLE': "Taking empty set from set via input types (List input)",
    'refSTE': "Taking empty set from set via input types (Tuple input)",

    'refSSI': "Taking intersection set from set via input types (Set input)",
    'refSLI': "Taking intersection set from set via input types (List input)",
    'refSTI': "Taking intersection set from set via input types (Tuple input)",

    ### Reference test items for Attributes
    'refCAV': "Taking Attribute (that exists) from Class ('volume')",
    'refCAC': "Taking Attribute (that exists) from Class ('colour')",
    'refCN': "Taking Attribute that does not exist from Class (warn, can be made to raise error)",
}
#############################################
########## Test Reference Creation ##########
#############################################
refCreateTestParams = [(refCreateArgs[key], refCreateFailStr[key], key) for key in refCreateArgs]

@pytest.mark.parametrize("refArgs,failStr,key", refCreateTestParams)
def test_reference_creation(refArgs: tuple, failStr: str, key: str):
    try:
        reference_create(*refArgs)
    except Exception as e:
        assert False, f"Failed: {failStr}, {key}. Error: {e}"


def reference_create(source: str, path: list[Any] | None) -> Reference:
    return Reference(source, path)


###################################################################################
##################### REFERENCES/MODELENGINE INTEGRATION TEST #####################
###################################################################################
######################################################
########## Test Reference Resolution Assets ##########
######################################################
# Keys to pick References for resolution test
refResolveKeys = {
    ### Test and Assertions for Empty/None Path
    'EP': 'refTET',
    'NP': 'refLNL',

    ### Test and Assertions for pd.Series
    'SeSiSe': 'refSeSiSe',
    'SeSnSe': 'refSeSnSe',
    'SeIV': 'refSeIV',
    'SeNV': 'refSeNV',

    ### Test and Assertions for pd.DataFrame
    'DFCSe': 'refDFCSe',
    'DFCV': 'refDFCV',
    'DFCDFS': 'refDFCDFS',
    'DFCDFM': 'refDFCDFM',
    'DFRnSe': 'refDFRnSe',
    'DFRiSe': 'refDFRiSe',
    'DFRV': 'refDFRV',
    'DFRiDFS': 'refDFRiDFS',
    'DFRiDFM': 'refDFRiDFM',
    'DfRnDF': 'refDfRnDF',
    
    ### Test and Assertions for dict
    'DV': 'refDV',
    'DTV': 'refDTV',
    'DTD': 'refDTD',
    'DLDs': 'refDLDs',
    'DLDi': 'refDLDi',

    ### Test and Assertions for ordered Iterables (list, tuple, range)
    'LV': 'refLV',
    'LSL': 'refLSL',
    'TV': 'refTV',
    'TST': 'refTST',
    'RV': 'refRV',
    'RSR': 'refRSR',

    ### Test and Assertions for References
    'RefSe': 'refRefSe',

    ### Test and Assertions for Deferreds
    'DefRef': 'refDefRef',

    ### Test and Assertions for Numbers
    'N': 'refN',

    ### Test and Assertions for Strings
    'StrC': 'refStrC',
    'StrS': 'refStrS',

    ### Test and Assertions for Callables
    'CallNN': 'refCallNN',
    'CallNA': 'refCallNA',
    'CallNK': 'refCallNK',
    'CallNM': 'refCallNM',

    'CallLN': 'refCallLN',
    'CallLA': 'refCallLA',
    'CallLK': 'refCallLK',
    'CallLM': 'refCallLM',

    ### Test and Assertions for Sets
    'SSO': 'refSSO',
    'SLO': 'refSLO',
    'STO': 'refSTO',

    'SSP': 'refSSP',
    'SLP': 'refSLP',
    'STP': 'refSTP',

    'SSE': 'refSSE',
    'SLE': 'refSLE',
    'STE': 'refSTE',

    'SSI': 'refSSI',
    'SLI': 'refSLI',
    'STI': 'refSTI',

    ### Test and Assertions for Attributes
    'CAV': 'refCAV',
    'CAC': 'refCAC',
    'CN': 'refCN'
}

# Expected values for resolution test
refResolveExpected = {
    ### Test and Assertions for Empty/None Path
    'EP': (100,200,400,800,1600),
    'NP': [99,-11,1000,-123],

    ### Test and Assertions for pd.Series
    'SeSiSe': pd.Series(data=[1,10,-20], index=pd.Index(['a','c','d'], dtype='object') ), # Forced dtype='object' as originally so
    'SeSnSe': pd.Series(data=[1,-20,50], index=['a','d',5]),
    'SeIV': 1,
    'SeNV': 0,

    ### Test and Assertions for pd.DataFrame
    'DFCSe': pd.Series(data=[0,13,"axs"],name='c3',index=["aa","bb","cc"]),
    'DFCV': "axs",
    'DFCDFS': pd.DataFrame(data=[1,10,"testval"],columns=['c1'],index=["aa","bb","cc"]),
    'DFCDFM': pd.DataFrame(data=[[1,1],[10,91],["testval", "night"]],columns=['c1','c5'],index=["aa","bb","cc"]),
    'DFRnSe': pd.Series(data=[10,-45,13,234,91,63,23], name="bb",index=['c1','c2','c3','c4','c5','c6','c7'], dtype=object),
    'DFRiSe': pd.Series(data=["testval","min","axs","as","night","asqweq","a"], name="cc",index=['c1','c2','c3','c4','c5','c6','c7'], dtype=object),
    'DFRV': 234,
    'DFRiDFS': pd.DataFrame(data=[[10,-45,13,234,91,63,23]],columns=['c1','c2','c3','c4','c5','c6','c7'], index=['bb'], dtype=object),
    'DFRiDFM': pd.DataFrame(data=[[10,-45,13,234,91,63,23],["testval","min","axs","as","night","asqweq","a"]],columns=['c1','c2','c3','c4','c5','c6','c7'], index=['bb','cc'], dtype=object),
    'DfRnDF': pd.DataFrame(data=[[1,0,0,0,1,1,0],["testval","min","axs","as","night","asqweq","a"]],columns=['c1','c2','c3','c4','c5','c6','c7'], index=['aa','cc'], dtype=object),
    
    ### Test and Assertions for dict
    'DV': True,
    'DTV': -11,
    'DTD': {'p1':0, 'p3':round, 'p4':[2,65]},
    'DLDs': {'p1':0, 'p5':range(0,5), 'p6': abs, ('p1','p2'):-11},
    'DLDi': {'p2':True, 'p5':range(0,5)},

    ### Test and Assertions for ordered Iterables (list, tuple, range)
    'LV': 1000,
    'LSL': [-11,1000,-123],
    'TV': 200,
    'TST': (400,800,1600),
    'RV': 10 + 2*10,
    'RSR': range(10+2*1, 10+2*12, 2*3),

    ### Test and Assertions for References
    'RefSe': pd.Series(data=[0,13,"axs"],name='c3',index=["aa","bb","cc"]),

    ### Test and Assertions for Deferreds
    'DefRef': Reference(source="ser",path=[[0,2,3]]),

    ### Test and Assertions for Numbers
    'N': 1000,

    ### Test and Assertions for Strings
    'StrC': 'h',
    'StrS': 'is a ',

    ### Test and Assertions for Callables
    'CallNN': "Hello World",
    'CallNA': 9,
    'CallNK': 11,
    'CallNM': 13,

    'CallLN': "Hi world",
    'CallLA': 8,
    'CallLK': 64,
    'CallLM': 81,

    ### Test and Assertions for Sets
    'SSO': {1, 2, 3, 4, 5, 6, 7, 8, 9},
    'SLO': {1, 2, 3, 4, 5, 6, 7, 8, 9},
    'STO': {1, 2, 3, 4, 5, 6, 7, 8, 9},

    'SSP': {1, 3, 5, 7, 9},
    'SLP': {1, 3, 5, 7, 9},
    'STP': {1, 3, 5, 7, 9},

    'SSE': set(),
    'SLE': set(),
    'STE': set(),

    'SSI': {1, 3, 9},
    'SLI': {1, 3, 9},
    'STI': {1, 3, 9},

    ### Test and Assertions for Attributes
    'CAV': 24,
    'CAC': "green",
    'CN': TestClass(2,3,4)
}

# Fail strings for Reference resolution test
refResolveFailStr = {
    ### Test and Assertions for Empty/None Path
    'EP': "Picking Tuple by using an empty path iterable",
    'NP': "Picking List by using a None path",

    ### Test and Assertions for pd.Series
    'SeSiSe': "Picking subset pd.Series from pd.Series by taking row index (.iloc) in nested list",
    'SeSnSe': "Picking subset pd.Series from pd.Series by taking row name (.loc) in nested list",
    'SeIV': "Picking value from pd.Series by taking row index (.iloc)",
    'SeNV': "Picking value from pd.Series by taking row name (.loc)",

    ### Test and Assertions for pd.DataFrame
    'DFCSe': "Picking pd.Series from pd.DataFrame by taking column",
    'DFCV': "Picking value from pd.DataFrame by taking column then data point",
    'DFCDFS': "Picking subset pd.DataFrame from pd.DataFrame by taking a single column in nested list",
    'DFCDFM': "Picking subset pd.DataFrame from pd.DataFrame by taking multiple columns in nested list",
    'DFRnSe': "Picking pd.Series from pd.DataFrame by taking row name (.loc)",
    'DFRiSe': "Picking pd.Series from pd.DataFrame by taking row index location (.iloc)",
    'DFRV': "Picking value from pd.DataFrame by taking row then data point",
    'DFRiDFS': "Picking subset pd.DataFrame from pd.DataFrame by taking a single row index (.iloc) in nested list",
    'DFRiDFM': "Picking subset pd.DataFrame from pd.DataFrame by taking multiple row index (.iloc) subset",
    'DfRnDF': "Picking subset pd.DataFrame from pd.DataFrame by taking row name (.loc) in nested list",

    ### Test and Assertions for dict
    'DV': "Picking value from dictionary",
    'DTV': "Picking value from dictionary by using a tuple key",
    'DTD': "Picking subset dictionary from dictionary by using a tuple key",
    'DLDs': "Picking subset dictionary from dictionary by using a list",
    'DLDi': "Picking intersection dictionary from dictionary by using a list",

    ### Test and Assertions for ordered Iterables (list, tuple, range)
    'LV': "Picking value from list",
    'LSL': "Slicing list from list",
    'TV': "Picking value from tuple",
    'TST': "Slicing tuple from tuple",
    'RV': "Picking value from range",
    'RSR': "Slicing range from range",

    ### Test and Assertions for References
    'RefSe': "Picking pd.Series by resolving Reference using ModelEngine",

    ### Test and Assertions for Deferreds
    'DefRef': "Picking Reference by resolving Deferred",

    ### Test and Assertions for Numbers
    'N': "Picking number from Reference source with further path",

    ### Test and Assertions for Strings
    'StrC': "Picking single character from string",
    'StrS': "Picking substring from string via slice",

    ### Test and Assertions for Callables
    'CallNN': "Picking result from named function with no arguments",
    'CallNA': "Picking result from named function with positional arguments",
    'CallNK': "Picking result from named function with keyword arguments",
    'CallNM': "Picking result from named function with mixed positional and keyword arguments",

    'CallLN': "Picking result from lambda function with no arguments",
    'CallLA': "Picking result from lambda function with positional arguments",
    'CallLK': "Picking result from lambda function with keyword arguments",
    'CallLM': "Picking result from lambda function with mixed positional and keyword arguments",

    ### Test and Assertions for Sets
    'SSO': "Picking original set from set via set input",
    'SLO': "Picking original set from set via list input",
    'STO': "Picking original set from set via tuple input",

    'SSP': "Picking partial set from set via set input",
    'SLP': "Picking partial set from set via list input",
    'STP': "Picking partial set from set via tuple input",

    'SSE': "Picking empty set from set via set input",
    'SLE': "Picking empty set from set via list input",
    'STE': "Picking empty set from set via tuple input",

    'SSI': "Picking intersection set from set via set input",
    'SLI': "Picking intersection set from set via list input",
    'STI': "Picking intersection set from set via tuple input",

    ### Test and Assertions for Attributes
    'CAV': "Picking volume attribute from class",
    'CAC': "Picking colour attribute from class",
    'CN': "Picking non-existent attribute from class"
}


refResolveTestParams = [(refCreateArgs[value], refResolveExpected[key], refResolveFailStr[key], key) for key, value in refResolveKeys.items()]
############################################
########## Test Reference Resolve ##########
############################################
@pytest.mark.parametrize("refArgs, expected, failStr, key", refResolveTestParams)
def test_reference_resolve(refArgs: tuple, expected: Any, failStr: str, key: str):
    # Test ModelEngine
    eng = ModelEngine(assets=test_assets, aliases = test_aliases)
    reference = reference_create(*refArgs)
    resolved = reference.resolve(eng)

    try:
        if isinstance(expected, pd.DataFrame):
            pd.testing.assert_frame_equal(resolved, expected)
        elif isinstance(expected, pd.Series):
            pd.testing.assert_series_equal(resolved, expected)
        else:
            assert resolved == expected, f"{failStr}, {key}"
    except AssertionError as e:
        assert False, f"Failed: {failStr}, {key}."