from collections.abc import Callable
from collections.abc import Iterator
import functools
from types import FunctionType

import pyoxigraph as ox

from triplestar_core.conversions import from_rdf_literal
from triplestar_core.conversions import to_rdf_literal

_RegisteredFunc = FunctionType


class FunctionRegistry:
    """Registry for SPARQL ``fn:`` extension functions.

    Input ``pyoxigraph.Literal`` arguments are automatically converted to
    their Python values via ``from_ox().value``, so function bodies receive
    native types (``ShapelyPoint``, ``float``, ``int``, …).
    Return values are converted back to RDF literals via
    :func:`~triplestar_core.msg_to_rdf.to_rdf_literal`.
    """

    def __init__(self) -> None:
        self._functions: dict[str, _RegisteredFunc] = {}

    def register(self, func: _RegisteredFunc, name: str | None = None) -> _RegisteredFunc:
        key = name or func.__name__
        if key in self._functions:
            raise ValueError(f"Function '{key}' is already registered")
        self._functions[key] = self._wrap(func)
        return func

    def get(self, name: str, default: _RegisteredFunc | None = None) -> _RegisteredFunc | None:
        return self._functions.get(name, default)

    def __getitem__(self, name: str) -> _RegisteredFunc:
        return self._functions[name]

    def __contains__(self, name: str) -> bool:
        return name in self._functions

    def __len__(self) -> int:
        return len(self._functions)

    def __iter__(self) -> Iterator[tuple[str, _RegisteredFunc]]:
        return iter(self._functions.items())

    def __repr__(self) -> str:
        return f'<FunctionRegistry({len(self)}): {", ".join(sorted(self._functions))}>'

    @staticmethod
    def _wrap(func: _RegisteredFunc) -> _RegisteredFunc:
        @functools.wraps(func)
        def wrapper(*args: ox.Literal, **kwargs: ox.Literal) -> ox.Literal | ox.NamedNode | None:
            mapped_args = tuple(from_rdf_literal(a) for a in args)
            mapped_kwargs = {k: from_rdf_literal(v) for k, v in kwargs.items()}

            result = func(*mapped_args, **mapped_kwargs)

            if result is None or isinstance(result, (ox.Literal, ox.NamedNode)):
                return result

            converted = to_rdf_literal(result)
            if converted is None:
                raise TypeError(
                    f"'{func.__name__}' returned unconvertible type {type(result).__name__}"
                )
            return converted

        return wrapper  # type: ignore


registry = FunctionRegistry()


def kb_function(name: str | None = None) -> Callable[[_RegisteredFunc], _RegisteredFunc]:
    return lambda func: registry.register(func, name=name)
