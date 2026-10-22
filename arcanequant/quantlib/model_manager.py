# Annotations import (to prevent issues when referring to classes that are not defined yet)
from __future__ import annotations

import hashlib
import inspect
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass, field, fields
from operator import attrgetter
from pathlib import Path
from typing import Any
from warnings import warn

from typeguard import typechecked

from .operation import RegisteredFunction
from .resolution import Deferred, Reference


@dataclass
class ModelEngine:
    assets: dict = field(default_factory=dict)
    aliases: dict = field(default_factory=dict)
    functions: dict = field(default_factory=dict)
    processes: dict = field(default_factory=dict)
    pipelines: dict = field(default_factory=dict)


    # Utility functions
    def check_hash_match(self, operation: Callable) -> bool:
        """
        Utility function to check if hash of inputted arg operation matches the existing Callable's signature. Returns True
        if the (underlying) function is an inbuilt, or its hash id matches the existing function in the engine, else False.
        """
        # If RegisteredFunction, do this, otherwise it is Callable
        if isinstance(operation, RegisteredFunction):
            opName: str = operation.name
            
            # If not builtin, return True if new function, or hash matches (i.e. same implementation)
            # This logic is all in one as it needs to be lazily evaluated using and (otherwise raises errors)
            if (operation.source_info.module_name != 'builtins'):
                alreadyExists: bool = opName in self.functions
                opHashId = operation.source_info.hash_id

                return not ( (alreadyExists) and ( (
                        isinstance(self.functions[opName], RegisteredFunction) and
                        (opHashId != self.get_registeredFunction(opName).source_info.hash_id)
                    ) or (
                        isinstance(self.functions[opName], Callable) and
                        (opHashId != RegisteredFunction(self.get_function(opName)).source_info.hash_id)
                    ) ) )
            
        else:
            # Here onwards it is a non-RegisteredFunction Callable
            # If operation not builtin, check if function key name exists and if implementation is exactly the same, if not return True
            opName: str = operation.__name__
            if not (
                inspect.isbuiltin(operation) or 
                (isinstance(operation, type) and getattr(operation, '__module__', None) == 'builtins')
            ):
                opSourceCode = inspect.getsource(operation)
                opHashId = hashlib.sha256(opSourceCode.encode()).hexdigest()

                # If function exists already and if implementation is the same, return False (should raise outside this function)
                alreadyExists: bool = opName in self.functions
                # This logic is all in one as it needs to be lazily evaluated using and (otherwise raises errors)
                return not ( (alreadyExists) and ( (
                        isinstance(self.functions[opName], RegisteredFunction) and
                        (opHashId != self.get_registeredFunction(opName).source_info.hash_id)
                    ) or (
                        isinstance(self.functions[opName], Callable) and
                        (opHashId != RegisteredFunction(self.get_function(opName)).source_info.hash_id)
                    ) ) )
            
        return True # If builtin or new function or hash matches


    ### MAKE CODE TO SAVE THE MODELENGINE, THE DATA, PARAMETERS/ARGS AND THE FUNCTIONS EVEN (if not inbuilt)

    def assign(self, name: str, obj: Any) -> None:
        """
        Assigns object to ModelEngine. Assigns to self.assets unless:
            - The object is a Reference or Deferred Reference, in which case it assigns to self.aliases.
            - The object is a Process, in which case it assigns the Process to self.processes.
            - The object is a Pipeline, in which case it assigns the Pipeline to self.pipelines.
            - The object already a RegisteredFunction, in which case it assigns it to self.functions.
            - The object is a function, in which case it registers the function and assigns to self.functions.

        Note: If the name already exists in the identified target attribute, it is overwritten.
        """
        if isinstance(obj, (Reference, Deferred)):
            self.aliases[name] = obj

        elif isinstance(obj, Process):
            self.processes[name] = obj

        elif isinstance(obj, Pipeline):
            self.pipelines[name] = obj

        elif isinstance(obj, RegisteredFunction) or callable(obj):
            self.functions[name] = obj
        
        else:
            self.assets[name] = obj


    def force_assign(self, tgt: str, name: str, obj: Any) -> None:
        """
        Forcefully assigns (name, obj) key/value pair to tgt attribute (e.g. tgt['name'] = obj).
        Note: If name exists in tgt, it is overwritten.
        """
        tgt_var = getattr(self, tgt) # Should be dict
        tgt_var[name] = obj
    
    
    def ref(self, alias: str) -> Reference:
        return self.aliases[alias]


    # RegisteredFunction-use functionalities
    def register_function(self, function: Callable) -> RegisteredFunction:
        """
        Creates, saves and returns a registered form of function (RegisteredFunction) to the self.functions dictionary.
        """
        functionRegistered = RegisteredFunction(function)  
        
        self.force_assign('functions', function.__name__, functionRegistered)
        return functionRegistered

    def get_function(self, name: str) -> Callable:
        """
        Returns the callable object from functions. 
        """
        if isinstance(self.functions[name], RegisteredFunction): return self.functions[name].function
        return self.functions[name]
    
    def get_registeredFunction(self, name: str) -> RegisteredFunction:
        tgt = self.functions[name]
        if isinstance(tgt, RegisteredFunction):
            return tgt
        else:
            raise TypeError("Obtained target object is not a RegisteredFunction, use get_function on non-RegisteredFunction objects.")

    def unregister_function(self, name: str) -> Callable:
        """
        Removes a function from the self.functions dictionary. Returns the function removed.
        """
        if isinstance(self.functions[name], RegisteredFunction): return self.functions.pop(name).function
        return self.functions.pop(name)
    
    

    def add_process(self, process, standardise = False) -> None:
        """
        Registers the process and the contained callable (operation). Standardises the process (by standarising the callable into RegisteredFunction) if enabled.
        """
        if standardise:
            process = self.standardise_process(process)

        self.force_assign('processes', process.name, process)

    
    def remove_process(self, processName) -> Process:
        """
        Removes the process from the processes dictionary. Simple wrapper function.
        """
        return self.processes.pop(processName)


    def standardise_process(self, process: Process) -> Process:
        """
        Standardises the process by converting the contained callable (operation) to RegisteredFunction (if not
        already so) and applying it to the Process. Returns the (modified) Process.
        """
        registeredFunction = self.standardise_operation(process.operation)
        
        process.operation = registeredFunction

        return process

    
    def standardise_operation(self, operation) -> RegisteredFunction:
        """
        Standardises operations by converting callable operations to RegisteredFunction and registering under
        the function's object name (i.e. operation.__name__) if not done so. Returns the RegisteredFunction.
        """

        # Get operation from dictionary if string (convert to registered function if not done so)
        if isinstance(operation, str):
            # If RegisteredFunction, it is already standardised (already converted and assigned)
            try:
                return self.get_registeredFunction(operation)
            
            # If not registered function, needs conversion and reassignment
            except TypeError: 
                return self.register_function(self.get_function(operation))
                

        # If RegisteredFunction, check if already exists in different implementation
        elif isinstance(operation, RegisteredFunction):
            opName = operation.name
            if not self.check_hash_match(operation):
                raise KeyError(f"The function '{opName}' already exists in the functions dictionary with different implementation.")
            
            self.assign(opName, operation)
            return operation

        # If operation is just a callable, create its RegisteredFunction then recall this function (switches to case above).
        elif callable(operation):
            opName = operation.__name__

            if not self.check_hash_match(operation):
                raise KeyError(f"The function '{opName}' already exists in the functions dictionary with different implementation.")

            return self.register_function(operation)

        
        else:
            raise TypeError(f"The input operation is type {type(operation)}, not str or RegisteredFunction or Callable.")

    
    def executeProcess(self, processName: str, return_output = True) -> Any | None:
        """
        Locate and execute the process with the process name. References types in .input_args/.input_kwargs are immediately
        resolved before applied to the target process.function, while Deferred types are applied as References to the target
        function. Returns output of process if return_output = True.
        """
        prcs: Process = self.processes[processName]
        return prcs.execute(self, return_output)

    def OutputTarget(asset, key):
        # To be implemented later as part of re-writing a part of an existing asset
        raise NotImplementedError




@dataclass
@typechecked
class Process:
    """
    Class for applying a function to a set of inputs and a targeted output location.

    Process.operation can be string only as a pointer for future standardisation to fit a ModelEngine (using
    ModelEngine.standardise_operation() or ModelEngine.standardise_process()).

    Note: Process can be executed without an engine if the operation is a callable (not str) and inputs do not need processing
    (e.g. resolving References/Deferreds).
    """

    name: str
    operation: Callable | RegisteredFunction | str
    input_args: Iterable[Any] = field(default_factory=list)
    input_kwargs: dict[str, Any]  = field(default_factory=dict)
    output_dest: str = field(default_factory=str)

    def execute(self, engine: ModelEngine | None = None, return_output = True) -> Any | None:
        """
        Execute the process, using ModelEngine (engine) if provided. References types in self.input_args or
        self.input_kwargs are immediately resolved using the given engine before being applied to the target self.function,
        while Deferred types are applied as References to the target function. If return_output = True, returns the output as well.

        Note: If engine is not provided, execute() can work if the operation is a callable (not str) and inputs do not need
        processing (e.g. resolving References/Deferreds) and always returns the output.
        """

        out_name = self.output_dest

        inp_args = self.input_args
        inp_kwargs = self.input_kwargs

        def pre_exec_resolver(arg):
            " Returns resolved References, and Reference from Deferred references. Returns anything else as-is. "
            if isinstance(arg, Reference):
                return arg.resolve(engine)
            elif isinstance(arg, Deferred):
                return arg.get_promise()
            
            return arg

        def pre_exec_args(*args) -> tuple:
            " Prepares the arguments by resolving References, and converting Deferred references to References. "
            prepd_args = []
            for arg in args:
                new_arg = pre_exec_resolver(arg)
                prepd_args.append(new_arg)

            return tuple(prepd_args)

        def pre_exec_kwargs(**kwargs):
            " Prepares the keyword arguments by resolving References, and converting Deferred references to References. "
            prepd_kwargs = {}
            for kw, arg in kwargs.items():
                new_arg = pre_exec_resolver(arg)
                prepd_kwargs[kw] = new_arg

            if kwargs != prepd_kwargs:
                print('Deferred/Reference found')

            return prepd_kwargs

        # If no engine, must return output directly (and with no preparation for References/Deferreds)
        if not engine:
            return self.operation(*inp_args, **inp_kwargs)

        # Otherwise, engine given, prepare self.operation if string
        operation = self.operation # To avoid writing over process
        if isinstance(self.operation, str):
            operation = engine.get_function(operation)

        # Also prepare args/kwargs and output destination
        # Prepare args/kwargs by resolving References/Deferred
        if inp_args is not None:
            inp_args = pre_exec_args(*inp_args)
        
        if inp_kwargs is not None:
            inp_kwargs = pre_exec_kwargs(**inp_kwargs)

        # Get result
        result: Any = operation(*inp_args, **inp_kwargs)

        # Use output destination (if provided)
        if self.output_dest:
            engine.assets[out_name] = result
        
        if return_output or not self.output_dest:
            return result


class Pipeline:
    ...