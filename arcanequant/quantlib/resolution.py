# Annotations import (to prevent issues when referring to classes that are not defined yet)
from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from operator import attrgetter
from typing import Any
from warnings import warn

import pandas as pd
from typeguard import typechecked

"""
This file, resolution.py contains a set of classes useful for managing referencing functionalities for ArcaneQuant.
___________________________________
Classes:
> Reference - Provides a way to path into a selected dataset from a given whole data, and allow lazy execution.
> Deferred - Provides a way to circumvent resolution (such as in cases when used in Processes).
"""

@typechecked
@dataclass
class Reference:
    """
    A class to point to a set of data within ModelManager. Takes a starting point (Reference.source) and progressively
    paths into the target destination (using Reference.path). Calling this class after instantiating it attempts to resolve
    the pointer and return the data.
    This enables lazy evaluation of specific fields using dynamic, string-keyed data sources. Also allows the underlying
    data source for a key (e.g., 'price') to be swapped at runtime and operations to be re-executed seamlessly.
    """
    source: str
    path: Iterable[Any] | None

    def resolve_step(self, obj, step_path, raiseOnTrunk = False, raiseOnFail = False):
        """
        Recursively resolves to find the correct object given the access step_path.
        Begins from Reference.assets unless otherwise specified.

        Settings to:
            > Get a pointer or a (shallow) copy of the object (important for pd.DataFrames)
            > Raise error when encountering a trunk (non-empty step_path on a further resolvable variable like an int).
        """
        _EMPTY = object()
        next_obj = _EMPTY

        ### Resolving for current object type
        # Dictionary resolution
        if isinstance(obj, dict):

            # If step_path is list (i.e. unhashable iterable), use list to get a subset dict of original dict
            if isinstance(step_path, list):
                next_obj = { k: obj[k] for k in (obj.keys() & step_path) }

            # If step_path is tuple (i.e. hashable iterable) try as key, if error, use to get a subset dict of original dict
            elif isinstance(step_path, tuple):
                try:
                    next_obj = obj[step_path] # Try as key

                except KeyError:
                    next_obj = { k: obj[k] for k in (obj.keys() & step_path) } # Get subset dict

            # Otherwise, use step_path as key directly
            else:
                next_obj = obj[step_path]
        

        # Dataframe resolution
        elif isinstance(obj, pd.DataFrame):
            # Types int and string provide pd.Series, while iterables which provide slices into pd.DataFrames
            # In all cases, try obj[step_path] for column indexing first, then row indexing
            try: # Column search
                next_obj = obj[step_path] 

            except KeyError: # Row search
                try:
                    next_obj = obj.loc[step_path, :] # Row search by name
                    
                except (ValueError, KeyError):
                    next_obj = obj.iloc[step_path]#.copy() # Pure location indexing (only for int/Iterable[int])


        # Series resolution
        elif isinstance(obj, pd.Series):
            # Types int and string provide a value, while iterables which provide slices into pd.Series
            # In all cases, try obj[step_path] for column indexing first, then row indexing

            # Returning pd.Series
            try: # Direct Search
                next_obj = obj[step_path] 

            except KeyError: # Search by loc/iloc
                try:
                    next_obj = obj.loc[step_path]
                
                except (ValueError, KeyError):
                    next_obj = obj.iloc[step_path]#.copy() # Pure location indexing (only for int/Iterable[int])


        # Ordered iterable resolution 
        elif isinstance(obj, list | tuple | range):
            # Type of step_path must be int here
            # if not isinstance(step_path, int): raise TypeError("When resolving through list, the path (index) must be an int")
            next_obj = obj[step_path]


        # Reference resolution (can be given to shorten existing paths)
        elif isinstance(obj, type(self)):
            # Type of step_path must be ModelEngine 
            next_obj = obj.resolve(step_path)


        # Deferred resolution (converts to Reference)
        elif isinstance(obj, Deferred):
            # Type of step_path must be None only (will raise if raiseOnFail = True)
            if step_path is not None:
                if raiseOnFail:
                    raise TypeError("Path for Deferred object must be None.")
            
                else:
                    warn(RuntimeWarning("Path for Deferred object should be None."))

            next_obj = obj.resolve()


        # Number resolution
        elif isinstance(obj, int | float): # No resolution
            numberResolution = f"The object {obj} is a float or int, and cannot be indexed with access path {step_path}"
            if raiseOnTrunk:
                raise RuntimeError(numberResolution)
            else:
                warn(RuntimeWarning(numberResolution))
                return obj

        # String resolution
        elif isinstance(obj, str):
            next_obj = obj[step_path]


        # Function/Callable resolution (step_path must be (args, kwargs), of type (tuple, dict), or None/(None, None) for no arguments)
        elif callable(obj) and not isinstance(step_path, str):
            # Check for step_path to not be str as classes (attributes resolved for later at end)
            # can also be callable, but if looking for attributes, step_path must be a string.

            # Type checking and correction of step_path
            # If step_path is None, call with no arguments
            if step_path is None:
                next_obj = obj()

            # If step_path is not None, it must be a tuple of (args, kwargs)
            elif isinstance(step_path, tuple | list) and len(step_path) == 2:
                args, kwargs = step_path

                # Replace None with empty tuple/dict for args/kwargs
                if args is None: args = ()
                if kwargs is None: kwargs = {}

                # Fixing args/kwargs order if given in wrong order (e.g. kwargs first)
                if isinstance(args, dict) or isinstance(kwargs, tuple):
                    args, kwargs = kwargs, args
                
                # Type checking of args and kwargs
                if not isinstance(args, tuple) or not isinstance(kwargs, dict):
                    raise TypeError("When resolving through a callable, the path must be a tuple of (args, kwargs), where args is a tuple and kwargs is a dict.")
                
                next_obj = obj(*args, **kwargs)

            # If step_path is not a valid tuple (also not a string), raise an error
            else:
                raise TypeError("When resolving through a callable, the path must be a tuple of (args, kwargs), where args is a tuple and kwargs is a dict.")


        # Set resolution
        elif isinstance(obj, set):
            next_obj = obj.intersection(step_path)


        # Resolve by attrgetter(step_path) if next_obj is not found yet (or if obj is a class/object with attribute)
        if next_obj is _EMPTY:
            find_sub_obj = attrgetter(step_path) # Also resolves obj.attr1.attr2
            # Generic class/object resolution (attempt of last resort)
            if raiseOnFail:
                next_obj = find_sub_obj(obj)
            else:
                try:
                    next_obj = find_sub_obj(obj)
                except AttributeError:
                    noAttributeWarning = f"The object {obj} does not have attribute {step_path}, returning prior object."
                    warn(RuntimeWarning(noAttributeWarning))
                    return obj

        return next_obj
    

    def resolve(self, engine):
        """
        Recursively resolves to find the correct object given the access path.
        Begins from Reference.assets unless otherwise specified.

        Settings to:
            > Get a pointer or a (shallow) copy of the object (important for pd.DataFrames)
            > Raise error when encountering a trunk (non-empty path on a further resolvable variable like an int).
        """

        # Find root object
        if self.source in engine.assets:
            obj = engine.assets[self.source]

        elif self.source in engine.aliases:
            obj = engine.aliases[self.source].resolve(engine)

        elif self.source in engine.functions:
            obj = engine.functions[self.source]

        elif self.source in engine.processes:
            obj = engine.processes[self.source]
        
        elif self.source in engine.pipelines:
            obj = engine.pipelines[self.source]

        else:
            raise ReferenceError("Source not found in within assets, aliases, functions, processes or pipelines")
        
        # Set self.path to empty list if it is None
        if self.path is None:
            self.path = []

        # Follow path
        for step in self.path:
            obj = self.resolve_step(obj, step)

        return obj

    __call__: Callable = field(default=resolve)


@typechecked
@dataclass(frozen=True)
class Deferred:
    """
    Class wrapper for deferred References, used for special cases of lazy execution of References. When used as an
    argument or keyword argument, causes an executed Process to use the pointed Reference directly as an input,
    where otherwise the Reference is resolved and only then used as an input.
    """
    reference: Reference

    def resolve(self):
        return self.reference
    
    def get_promise(self):
        return self.resolve()

    __call__: Callable = field(default=resolve)