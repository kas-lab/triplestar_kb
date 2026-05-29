import functools
from types import FunctionType
from typing import Optional

from pyoxigraph import Literal as _RdfLiteral
from pyoxigraph import NamedNode as _NamedNode

from triplestar_core.msg_to_rdf import to_rdf_literal

_RegisteredFunc = FunctionType


class FunctionRegistry:
    """Registry for SPARQL ``fn:`` extension functions.

    Auto-converts return values: ``None`` → None, ``Literal``/``NamedNode``
    pass through, everything else goes through ``RosToRdfLiteralConverterRegistry``.
    """

    def __init__(self) -> None:
        self._functions: dict[str, _RegisteredFunc] = {}

    def register(self, func: _RegisteredFunc, name: Optional[str] = None) -> _RegisteredFunc:
        func_name = name or func.__name__
        if func_name in self._functions:
            raise ValueError(f"Function '{func_name}' is already registered")
        wrapped = self._auto_convert_return(func)
        self._functions[func_name] = wrapped
        return func

    def kb_function(self, name: Optional[str] = None):
        return lambda func: self.register(func, name=name)

    def get(self, name: str, default=None) -> Optional[_RegisteredFunc]:
        return self._functions.get(name, default)

    def __getitem__(self, name: str) -> _RegisteredFunc:
        return self._functions[name]

    def __contains__(self, name: str) -> bool:
        return name in self._functions

    def __len__(self) -> int:
        return len(self._functions)

    def __iter__(self):
        return iter(self._functions.items())

    def __repr__(self) -> str:
        names = ', '.join(sorted(self._functions))
        return f'<FunctionRegistry({len(self)}): {names}>'

    @staticmethod
    def _auto_convert_return(func: _RegisteredFunc) -> _RegisteredFunc:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            if result is None:
                return None
            if isinstance(result, (_RdfLiteral, _NamedNode)):
                return result
            converted = to_rdf_literal(result)
            if converted is None:
                raise TypeError(
                    f"Function '{func.__name__}' returned {type(result).__name__}, "
                    f'which could not be converted to an RDF literal.'
                )
            return converted

        return wrapper  # type: ignore


registry = FunctionRegistry()


def kb_function(name: Optional[str] = None):
    return registry.kb_function(name)
