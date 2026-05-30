"""SPARQL extension functions.

Arguments are auto-converted from RDF literals to Python types.

Return values are auto-converted to RDF literals. Return ``None`` for no result,
a ``Literal``/``NamedNode`` for full control, or a plain Python type otherwise.
"""

from triplestar_core.functions import kb_function


@kb_function('hello')
def hello(name) -> str:
    return f'Hello, {name.value}!'


@kb_function('add')
def add(a, b) -> int:
    return int(a.value) + int(b.value)


@kb_function()
def distance(geom_a, geom_b) -> float:
    return geom_a.distance(geom_b)
