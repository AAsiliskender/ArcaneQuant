import pandas as pd
from pathlib import Path
from typing import Iterable
from .DataManager import DataManager
from dataclasses import dataclass, asdict, fields, field

@dataclass
class ModelEngine():
    """
    ModelEngine is the class that manages data analysis based operations to DataFrames from storage.
    This class is/can be instantiated (per session), and used to set/select data for visual purposes or for creating or
    storing analytical models. Requires pandas.DataFrame.

    ___________________________________
    Class Attributes:
    > data - The DataFrame containing the data pool to be used for the model(s).
    > labels - A dictionary containing the name of the selection (key) that references one or more labels (i.e. columns) of the subset
    (value) to be selected from the larger dataset (self.data and self.modelsDict).
    > ops - A dictionary of stored and named (key) anonymous lambda operations (value) available in the engine instance.
    > processDict - A dictionary of named 'processes' (key) that reference a tuple (value) of an operation (ops) and its corresponding
    selection label(s).
    > pipelineDict - A dictionary of named (key) pipelineDict referencing an iterable (value) of tuples containing the names
    of operations to be done in succession. The operations must be ordered correctly and the input/output variable count must be correct.
    > modelsDict - A dictionary containing the name (key) and output (value) of processDict elements after execution.

    ___________________________________
    Method List:
    > inherit_from - Inherits one, some or all of its attributes from the target ModelEngine.
    > merge - Merges two instances of ModelEngine, prioritising attributes from primary source.
    > update_labels - Updates labels by adding all columns in ModelEngine.data and keys in ModelEngine.modelsDict
    > execute_process - Executes a specific item in processDict (i.e. one process) by running the relevant pipeline on data
    filtered using ModelEngine.labels (can store output into ModelEngine.modelsDict).
    > execute_pipeline - Executes specified pipeline's processes in order, stores output into ModelEngine.modelsDict.
    > execute_all_pipe - Executes all pipelines in pipelineDict.
    > models - Outputs the models from self.modelsDict in pandas.DataFrame format.

    ___________________________________
    Notes:
    - Avoid using the same name for processes and pipelines (as, if storing into self.modelsDict, it can overwrite).
    - execute_process() can be used to obtain an output of a process directly if inplace=False. 
    - To apply a Pandas process (i.e. lambda x: x.diff()) that can be applied to one column, to multiple columns in one go, wrap
    the columns list marked in labels in a 1-tuple (e.g. (['X', 'Y'],) -- or use
    pandas.DataFrame.apply(ModelEngine.ops[operationName]) instead.
    - The inherit_from function can be set to inherit specific attributes, or, take shallow or deep copies.
    - execute_all_pipe executes all pipelines in order of keys in pipelineDict.
    - ModelEngine.labels uses a string key link to a string or string iterable value, to allow for referencing multiple columns
    (or all columns) in a single key.
    

    """
    data: pd.DataFrame = field(default_factory=pd.DataFrame)
    labels: dict = field(default_factory=dict)
    ops: dict = field(default_factory=dict)
    processDict: dict = field(default_factory=dict)
    pipelineDict: dict = field(default_factory=dict)
    modelsDict: dict = field(default_factory=dict)

    # Function immediately called after init
    def __post_init__(self):
        # Update labels on init (then at end of every executed process)
        self.update_labels()

    def inherit_from(self, other, attribs: str | Iterable[str] = None, method: str = 'deep'):
        """
        Inherits attributes from another ModelEngine (in-place).
        
        Inputs:
        - other - The other ModelEngine to inherit attributes from.
        - attribs - String or Iterable of string indicating which attributes are inherited.
        - method - String indicating method of inheritance. 'deep' uses deep copies, 'shallow' uses
        shallow copies, while 'assign' uses reference pointers to point to the same object.

        Notes:
        > If attribs is None, inherits all attributes.
        > If an attribute exists in both instances, the attribute from 'other' overwrites the
        attribute in 'self'.
        """
        import copy

        # Normalising attribs into list of field names
        if attribs is None:
            # If attribs is None (inherit all)
            attribs = [attr.name for attr in fields(other)]
        elif isinstance(attribs, str):
            # If attribs only string wrap in list
            attribs = [attribs]
        elif isinstance(attribs, Iterable):
            # If attribs is iterable of string (inherit only these attributes), convert to list
            attribs = list(attribs)            
        else:
            # If attribs not valid
            raise ValueError("You must provide a valid attribs input (None, str or Iterable[str])")

        # Now inherit attributes
        for attr in attribs:
            value = getattr(other, attr)
            if method == 'deep':
                setattr(self, attr, copy.deepcopy(value))
            elif method == 'shallow':
                setattr(self, attr, copy.copy(value))
            elif method == 'assign':
                setattr(self, attr, value)
            else:
                raise ValueError("You must select the correct inheritance method.")

    @classmethod
    def merge(cls, primary, secondary, method = 'deep'):
        """
        Non-destructively merges attributes from two ModelEngines, outputs another ModelEngine.
        
        Inputs:
        - primary - The first ModelEngine to merge attributes from (merge priority).
        - secondary - The second ModelEngine to merge attributes from.
        - method - String indicating method of merge. 'deep' merges using deep copies, 'shallow'
        merges using shallow copies, while 'assign' uses reference pointers to point to the same object.

        Notes:
        > If attribs is None, merges all attributes.
        > If an attribute exists in both instances, the attribute from 'base' has priority.
        """
        import copy

        # Get primary attributes
        primAttribs = [attr.name for attr in fields(primary)]
        # Get secondary attributes and filter out ones in primary
        secAttribs = [attr.name for attr in fields(secondary) if attr.name not in primAttribs]

        merged = cls()

        # Inherit attributes from both (secondary first here in case there is overlap, then primary overwrites)
        if method == 'deep': # Get each value and save deep copy
            for attr in secAttribs:
                value = getattr(secondary, attr)
                setattr(merged, attr, copy.deepcopy(value))
            for attr in primAttribs:
                value = getattr(primary, attr)
                setattr(merged, attr, copy.deepcopy(value))

        elif method == 'shallow': # Get each value and save shallow copy
            for attr in secAttribs:
                value = getattr(secondary, attr)
                setattr(merged, attr, copy.copy(value))
            for attr in primAttribs:
                value = getattr(primary, attr)
                setattr(merged, attr, copy.copy(value))

        elif method == 'assign': # Get each value and assign it
            for attr in secAttribs:
                value = getattr(secondary, attr)
                setattr(merged, attr, value)
            for attr in primAttribs:
                value = getattr(primary, attr)
                setattr(merged, attr, value)

        else:
            raise ValueError("You must select the correct inheritance method.")

    def update_labels(self):
        """Updates labels by adding all columns in ModelEngine.data and keys in ModelEngine.modelsDict"""
        updateDict = {}
        for col in self.data.columns.to_list():
            updateDict[col] = col

        # Use self.models() to get all models output in a single dataframe to get columns (may not be that efficient)
        modelCols = list(self.models().columns)        

        for modelName in modelCols:
            updateDict[modelName] = modelName

        #for modelName in list(self.modelsDict.keys()):
        #    updateDict[modelName] = modelName

        self.labels |= updateDict


    def execute_process(self, processName: str, inplace: bool = True, echo = True):
        """
        Executes a specific item in processDict (i.e. one process) by running the relevant pipeline on data
        filtered using self.labels. Can adds to/update ModelEngine.modelsDict if "inplace" enabled, returns
        pandas.Series otherwise.
        
        Inputs:
        - processName - String of the process dictionary name to be executed.
        - inplace - Bool indicating to update ModelEngine.modelsDict or return pandas.Series.

        Note:
        - This function can be used in a single-cast or multi-cast state.
        - Single casts are just a function taking one argument and using a simple label (string) pointing to a column name.
            > Example: if process points to operation lambda x: 2*x, and label "A", this executes lambda function on column "A"
        - You can multi-cast a process on multiple columns/groups:
            > To cast a single argument function on 'n' columns, use a label of tuple length 1, containing an iterable of length
            n (e.g. if lambda x: 2*x on 3 columns, then a label can be (["a", "b", "c"],))
            > To cast a multi-argument ('m' arguments) function on 'n' columns, use a label of tuple length m, containing
            iterables each of length n (e.g. if lambda x,y: x+y on 3 columns, then label can be (["a","c","e"], ["b","d","f"]),
            this would add "a" with "b", "c" with "d" and so on.)
        """
        # Process (operationName, labelName)
        operationName, labelName = self.processDict[processName] # labelName can be string or iterable
        
        ### Get data label from labelName
        # Note: Here we allow for multiple labels (each label can even indicate more than one column)
        dataCols = []
        if isinstance(labelName, str): # If one label given
            dataCols = self.labels[labelName]
        else: # If multiple given, unpack as needed (if indicates more than one column) and stack up
            ################# TODO: FIX THIS TO ALLOW MULTI-CAST MULTI-ARG ETC.
            ################# UNPACK ALL IF 1 ARG OP (IF MULTI-CAST MAKE SURE COL NAMES ARE GOOD), IF MULTIPLE ARG AND UNPACKED COL COUNT = ARG -> UNPACK.
            ################# IF MULT ARG AND UNPACKED COL COUNT = k*ARG COUNT,  THEN PACK 1 LEVEL HIGHER AND USE AS MULTICAST
            for label in labelName:
                labelComp = self.labels[label]

                # If label indicates one column
                if isinstance(labelComp, str):
                    dataCols.append(labelComp)
                else: # If label indicates more than one column
                    for col in labelComp:
                        dataCols.append(col)

        # Pack dataCols if not iterable (to unpack and allow iterable use)
        if isinstance(dataCols, str): # i.e. label is direct string, no tuple or tuple(iterable) (1-arg, single cast)
            dataCols = [dataCols]
        elif not isinstance(dataCols, list):
            dataCols = list(dataCols) # Convert any non-list iterable to list

        # labelName above gets names of sets of columns (in their order if multiple), while the dataCols puts into all into a list (i.e. unpacks sets) 

        # Copy required data columns (shallow) and merge with relevant models data (in case model needs to reuse produced data)
        copyData = self.data.copy()
        copyModels = self.models()
        inputData = pd.concat([copyData, copyModels], axis=1) # Data pool to be used in process (to be slimmed down)

        # Error check: dataCols names must all be in column list of inputData (otherwise data is missing)
        tempDataColSet = set(inputData.columns)
        requestedColSet = set(dataCols)
        if tempDataColSet.intersection(requestedColSet) != requestedColSet:
            raise ValueError("The required data columns indicated by the labels is missing from the dataset.")

        # Select columns of inputData based on input and distinguish single and multi-cast use
        multiCast = False # Trigger for multicast labelling (operation_input_columns)
        ################# TODO: THIS MAY BE UNNECESSARY SINCE ALL LABELS AND ASSOCIATED COLS ARE UNPACKED BEFOREHAND ~LINE 210
        if all(isinstance(label, str) for label in dataCols):
            # If str or iterable(str), then single-cast (any argument count)
            inputData = inputData[dataCols]

        elif all(isinstance(label, Iterable) for label in dataCols):
            # Iterable of labels for multicasting - execute multiple times
            import itertools # Easier as method exists (and more optimised)
            multiCast = True

            # Get all dataCols in a single list (for filtering dataframe)
            labelFilter = list(itertools.chain(*dataCols))
            # Drop duplicates in list
            labelFilter = list(set(labelFilter))
            inputData = inputData[labelFilter]                 
        else:
            raise TypeError("The set of label(s) provided must be either all strings or all iterables")

        # Get process operation
        operation = self.ops[operationName]

        # Error check (if operation args count =/= datalabel length, raise error)
        if len(dataCols) != operation.__code__.co_argcount:
            raise ValueError(f"The process argument count ({operation.__code__.co_argcount}) does not match the inputted data label count ({len(dataCols)}).")

        # Get data subset from labels and separate into list (to use as arguments) 
        inputCols = []

        # Split each col and add to list to use as argument
        for colidx in range(len(inputData.columns)):
            inputCols.append(inputData.iloc[:,colidx])

        ### Execute process operation (set to a dict)
        result=[]

        # We now execute the operation based on if it is multi-casted or single-casted (inputCols or its derivative is in a list
        # to be unpacked for operation, allowing all n > 0 arguments)

        # Multi-casted operation (can also do a single-cast in this as well)
        if all(isinstance(x, pd.DataFrame) for x in inputCols):
            # Separate n-dataframes of m columns into m groups of n columns (for n-arg function, casted m times)
            if echo:
                print(f"Multi-casting Function: {processName}")
            
            colCounts = [inp.shape[1] for inp in inputCols]
            # Guard function for mismatched column counts in dataframes
            if len(set(colCounts)) != 1:
                raise TypeError("The inputted column groups for the multi-argument function must be the same length") 
            
            # Group all corresponding columns before applying to function
            combos = list(zip(*dataCols))

            # Pick each column from each dataframe and group, and execute it
            result = []
            for combo in combos:
                group = []
                uniqueId = ""
                for i in range(len(combo)):
                    col = combo[i]
                    uniqueId += "_"+str(col)
                    group += [inputCols[i][col]]

                if echo:
                    print(f"Casting \"{processName}\" on column combination {combo}")
                resultPart = operation(*group) # Execute operation when group ready
                resultPart.rename(uniqueId, inplace=True) # Rename to give unique identifier
                result += [resultPart]

            result = pd.concat(result, axis=1)
        
        # Single-casted operation
        elif all(isinstance(x, pd.Series) for x in inputCols):
            # If single-argument
            if echo:
                print(f"Single-casting Function \"{processName}\" on {dataCols}")

            result = operation(*inputCols) # *var unpacks columns to be used for function
            result.rename(processName, inplace=True) # Rename to process name

        else:
            # Raise if inputted column groups are mismatched (must be all DataFrame or all Series, otherwise mismatch)
            raise TypeError("The inputted column groups for the multi-argument function must be the same length")        
        

        # If multicasted, add unique identifer to avoid improper overwriting
        if multiCast:
            resultCols = list(result.columns)
            for i in range(len(resultCols)):
                resultCols[i] = f"{processName}{resultCols[i]}"
            renameDict = dict( zip(list(result.columns), resultCols) )

            result.rename(mapper = renameDict, axis=1, inplace=True)

        # Return option
        if inplace:
            self.modelsDict[processName] = result

            # Update labels at end
            self.update_labels()
        else:
            return result
    
    def execute_pipeline(self, pipelineName: str, preserve: bool = True):
        """
        Executes a single pipeline item in pipelineDict by running all process in the pipeline in order. Adds
        the final process output to ModelEngine.model, but also adds all process outputs if 'preserve' enabled.
        
        Inputs:
        - pipelineName - String of the pipeline dictionary name to be executed.
        - preserve - Bool indicating to add all process outputs (instead of only the final process) to ModelEngine.model.
        """
        # If no preserve, copy self.modelsDict and self.labels into dummy, execute as if preserve exists, then reinstate
        # original dummy self.modelsDict and self.labels and add final process output
        # Odd workaround, but if it works...

        dummyModels = {}
        dummyLabels = {}
        if not preserve:
            dummyModels = self.modelsDict.copy()
            dummyLabels = self.labels.copy()

        pipeline = self.pipelineDict[pipelineName]

        # Pack pipeline if not iterable (to unpack and allow iterable use)
        if isinstance(pipeline, str):
            pipeline = [pipeline]
        elif not isinstance(pipeline, list):
            pipeline = list(pipeline) # Convert any non-list iterable to list

        for process in pipeline:
            self.execute_process(process, True) # Add process output to modelsDict

        # Workaround
        if not preserve:
            lastProcessName = pipeline[-1]
            lastProcess = self.modelsDict[lastProcessName]

            self.modelsDict = dummyModels
            self.modelsDict |= {lastProcessName : lastProcess}
            self.labels = dummyLabels
            self.labels[lastProcessName] = lastProcessName

        return

    def execute_all_pipe(self, preserve: bool = True):
        """
        Executes pipelines in pipelineDict (in order of key).

        Inputs:
        - preserve - Bool indicating to add each pipelines' process outputs (instead of only the final process).

        Note: Having preserve = False will raise errors if a pipeline depends on other pipelines' intermediate
        output(s) for information/labels.
        """

        for pipeName in list(self.pipelineDict.keys()):
            self.execute_pipeline(pipeName, preserve=preserve)
    
    def models(self):
        "Outputs the models from self.modelsDict in pandas.DataFrame format."

        # Separate the pd.Dataframes from pd.Series and concatenate them
        dfList = []
        dummyDict = self.modelsDict.copy()
        for key in list(dummyDict.keys()):
            if isinstance(dummyDict[key], pd.DataFrame):
                dfList.append(dummyDict.pop(key)) # Pop dataframes from self.modelsDict

        result = pd.DataFrame.from_dict(dummyDict) # All pd.Series into result 
        
        result = pd.concat([result] + dfList, axis=1) # Add dataframes to concatenate

        return result
    
    # Methods to add to backend interfacer
    # get_data - 
    # add_selection - 
    # del_selection - 
    # add_ops - 
    # del_ops - 
    # 