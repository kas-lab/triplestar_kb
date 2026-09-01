# Custom SPARQL functions

Python files in a bringup package's `functions/` directory can register functions callable from SPARQL under the `fn:` prefix.

## Define a function

Use `@kb_function()` to keep the Python name, or pass an explicit SPARQL function name:

```python
from triplestar_core.functions import kb_function


@kb_function()
def add(left: int, right: int) -> int:
    return left + right


@kb_function("distance")
def distance_between(first, second) -> float:
    return first.distance(second)
```

With `base_iri: "http://triplestar.local"`, call these functions as:

```sparql
PREFIX fn: <http://triplestar.local/functions/>

SELECT ?sum WHERE {
  BIND(fn:add(2, 3) AS ?sum)
}
```

## Conversion contract

The registry converts RDF literal arguments to Python values before calling the function. Common mappings include:

| RDF datatype | Python value |
| --- | --- |
| `xsd:integer` | `int` |
| `xsd:double`, `xsd:float` | `float` |
| `xsd:string` | `str` |
| `xsd:boolean` | `bool` |
| `xsd:dateTime` | `datetime.datetime` |
| `geo:wktLiteral` | A Shapely geometry |

A return value can be:

- `None`, which produces no RDF term
- A `pyoxigraph.Literal` or `pyoxigraph.NamedNode`, for explicit control
- A supported Python scalar or Shapely geometry, which is converted to an RDF literal

An unsupported return type raises a `TypeError` when the SPARQL function is evaluated. The complete conversion list is in [ROS to RDF conversions](../concepts/message-conversions.md).

## Loading behavior

During configure, the core node imports every top-level `*.py` file in `functions/`. Registration therefore happens as an import side effect. Function names must be unique in the process-wide registry.

Keep extension functions deterministic and free of ROS spinning or long blocking work. Use [query-time subscribers](configuration.md#query-time-topic-subscribers) when a function needs the latest topic or TF value.
