"""SPARQL extension functions.

Return values are auto-converted to RDF literals. Return ``None`` for no result,
a ``Literal``/``NamedNode`` for full control, or a plain Python type otherwise.
"""

from pyoxigraph import Literal
from triplestar_core.functions import kb_function


@kb_function('hello')
def hello(name: Literal) -> str:
    return f'Hello, {name.value}!'


@kb_function('add')
def add(a: Literal, b: Literal) -> int:
    return int(a.value) + int(b.value)
