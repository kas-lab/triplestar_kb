---
icon: lucide/arrow-left-right
---

# ROS → RDF conversion

ROS 2 defines its own message types for data like timestamps, points, integers, and polygons. To store these in the knowledge base, they need to be converted to typed RDF literals.

The conversion module (`triplestar_core.conversions`) handles this automatically — both forward (ROS → RDF) and reverse (RDF → Python / Shapely).

## Conversion table

| ROS message type | Python / Shapely type | RDF literal type |
|---|---|---|
| `geometry_msgs/Point`, `Point32`, `PointStamped` | `shapely.geometry.Point` | `geo:wktLiteral` |
| `geometry_msgs/Pose` | `shapely.geometry.Point` | `geo:wktLiteral` |
| `geometry_msgs/Vector3`, `Vector3Stamped` | `shapely.geometry.Point` | `geo:wktLiteral` |
| `geometry_msgs/Polygon`, `PolygonStamped`, `PolygonInstance`, `PolygonInstanceStamped` | `shapely.geometry.Polygon` | `geo:wktLiteral` |
| `builtin_interfaces/Time` | `datetime.datetime` | `xsd:dateTime` |
| `std_msgs/Float32`, `Float64` | `float` | `xsd:double` |
| `std_msgs/Int8`, `Int16`, `Int32`, `Int64` | `int` | `xsd:integer` |
| `std_msgs/UInt8`, `UInt16`, `UInt32`, `UInt64` | `int` | `xsd:integer` |
| `std_msgs/Char`, `Byte` | `int` / `str` | `xsd:integer` / `xsd:string` |
| `std_msgs/Bool` | `bool` | `xsd:boolean` |
| `std_msgs/String` | `str` | `xsd:string` |

## How it works

### Forward: `to_rdf_literal(msg)`

```python
from triplestar_core.conversions import to_rdf_literal

literal = to_rdf_literal(Point32(x=1.0, y=2.0, z=0.0))
# → ox.Literal("POINT (1 2)", datatype=geo:wktLiteral)

literal = to_rdf_literal(Float32(data=3.14))
# → ox.Literal("3.14", datatype=xsd:double)
```

For ROS messages, the function unwraps the `.data` field (for `std_msgs`) or converts geometry messages to Shapely objects, then serializes them using `rdflib`'s literal lexicalizers.

### Reverse: `rdf_literal_to_python(literal)`

```python
from triplestar_core.conversions import rdf_literal_to_python

point = rdf_literal_to_python(
    ox.Literal("POINT (1 2)", datatype=ox.NamedNode(GEO.wktLiteral))
)
# → shapely.geometry.Point(1, 2)

value = rdf_literal_to_python(
    ox.Literal("42", datatype=ox.NamedNode(XSD.integer))
)
# → 42
```

## Usage in Jinja2 templates

Insertion subscribers make the `rdf` filter available for converting ROS message fields inside templates:

```jinja2
{% raw %}
PREFIX ex: <http://example.org/>

INSERT DATA {
  ex:robot ex:position {{ msg.pose | rdf }} .
  ex:robot ex:battery {{ msg.battery | rdf }} .
}
{% endraw %}
```

## Usage in query-time subscribers

When a query-time subscriber returns a ROS message (or a field of it), the value is automatically converted via `to_rdf_literal()` before being returned from the SPARQL function.

## Usage in custom SPARQL functions

When you write a [custom SPARQL function](../bringup_package/functions.md), arguments are automatically converted from RDF literals to Python types, and return values are converted back:

```python
@kb_function("distance")
def distance(a: ShapelyPoint, b: ShapelyPoint) -> float:
    return a.distance(b)
```

## Shapely bindings

The module registers `geo:wktLiteral` ↔ `shapely.geometry.base.BaseGeometry` bindings with `rdflib`, so Shapely geometries are automatically serialized to/from WKT strings.
