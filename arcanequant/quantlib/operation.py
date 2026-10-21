# Annotations import (to prevent issues when referring to classes that are not defined yet)
from __future__ import annotations

import hashlib
import inspect
from collections.abc import Callable
from dataclasses import dataclass, field
from warnings import warn

from typeguard import typechecked

"""
This file, operation.py contains a set of classes useful for managing informational functionalities for ArcaneQuant.
___________________________________
Classes:
> RegisteredFunction - Provides a way to store information about a function (to save/record a specific implementation).
> SourceInfo - Container for important information regarding a callable/function (such as source code, module etc.).
"""

@typechecked
@dataclass(frozen=True)
class RegisteredFunction:
    " Saves the function and its source information while retaining its functionality."
    function: Callable
    source_info: SourceInfo = field(init=False)

    def __post_init__(self: RegisteredFunction):
        # If a C-compiled built-in function/method or a core built-in type/class (like int, str, dict), do not fully register
        if inspect.isbuiltin(self.function) or (isinstance(self.function, type) and getattr(self.function, '__module__', None) == 'builtins'):
            warn(RuntimeWarning("The registered function is a built-in function or method, only registering moduleName, hashId."))
            
            moduleName = 'builtins'
            hashId = hashlib.sha256(moduleName.encode()).hexdigest()

            object.__setattr__(
                self,
                "source_info",
                SourceInfo(module_name=moduleName, hash_id=hashId)
            )
        
        else: # Fully register
            sourceCode = inspect.getsource(self.function)
            sourceFile = inspect.getsourcefile(self.function)
            sourceLines = tuple(inspect.getsourcelines(self.function)[0])
            startLine = inspect.getsourcelines(self.function)[1]
            moduleName = inspect.getmodule(self.function).__name__ 
            hashId = hashlib.sha256(sourceCode.encode()).hexdigest()
        
            object.__setattr__(
                self,
                "source_info",
                SourceInfo(moduleName, sourceCode, sourceFile, sourceLines, startLine, hashId)
            )

    def __call__(self, *args, **kwargs):
        return self.function(*args, **kwargs)

    @property
    def name(self):
        return self.function.__name__

@typechecked
@dataclass(frozen=True)
class SourceInfo:
    """
    Contains the module name of the function and - if it is not an builtin function or method - source_code on save,
    source file/lines, where the source line starts on the file and the source code's hash ID.
    If it is an inbuilt, it contains only the module name = 'builtins'.

    Also contains the hash_id (to compare functions with).
    """
    module_name: str
    source_code: str = field(default_factory=str)
    source_file: str = field(default_factory=str)
    source_lines: str = field(default_factory=str)
    start_line: int = field(default_factory=str)
    hash_id: str = field(default_factory=str)
