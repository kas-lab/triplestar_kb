---
icon: lucide/code
---

# Custom SPARQL functions

You can extend the KB with custom Python functions callable directly from SPARQL queries using the `fn:` prefix.

## How it works

1. Place Python files in your bringup package's `functions/` directory
2. Use the `@kb_function` decorator to register functions
3. At configure time, `TriplestarKBNode` discovers and registers all functions
4. They become available as `fn:functionName(...)` in SPARQL

## Example

```python
from triplestar_core.functions import kb_function

@kb_function("hello")
def hello(name: str) -> str:
    return f"Hello, {name}!"


@kb_function("add")
def add(a: float, b: float) -> float:
    return a + b
```

Called in SPARQL:

```sparql
PREFIX fn: <http://triplestar.local/functions/>
SELECT ?greeting ?sum WHERE {
  BIND(fn:hello("World") AS ?greeting)
  BIND(fn:add(1, 2) AS ?sum)
}
```

## Type conversions

The function registry handles type conversion automatically:

| SPARQL argument type | Python type received |
|---|---|
| `xsd:integer` | `int` |
| `xsd:double`, `xsd:float` | `float` |
| `xsd:string` | `str` |
| `xsd:boolean` | `bool` |
| `geo:wktLiteral` | `shapely.geometry.Point` / `Polygon` |
| `xsd:dateTime` | `datetime.datetime` |

Return values are converted back to RDF literals using the same [ROS → RDF conversion](../concepts/ros-to-rdf.md) table.

## The `@kb_function` decorator

```python
from triplestar_core.functions import kb_function

# Registered with the function's name
@kb_function()
def my_function(...): ...

# Or with an explicit name
@kb_function("customName")
def my_function(...): ...
```

Functions are registered into a global `FunctionRegistry` instance (`triplestar_core.functions.registry`). The node iterates this registry during configuration and adds each function to the KB via `kb.add_kb_function()`.
