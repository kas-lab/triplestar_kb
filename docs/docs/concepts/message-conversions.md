# ROS to RDF conversions

TriplestarKB converts selected ROS messages and Python values to typed RDF literals. The same conversion layer is used by insertion templates, query-time functions, and custom function return values.

## Supported values

| Input | Python value used internally | RDF datatype |
| --- | --- | --- |
| `geometry_msgs/Point`, `Point32`, `PointStamped` | `shapely.Point` | `geo:wktLiteral` |
| `geometry_msgs/Pose` | Position as `shapely.Point` | `geo:wktLiteral` |
| `geometry_msgs/Vector3`, `Vector3Stamped` | `shapely.Point` | `geo:wktLiteral` |
| `geometry_msgs/Polygon`, `PolygonStamped` | `shapely.Polygon` | `geo:wktLiteral` |
| `geometry_msgs/PolygonInstance`, `PolygonInstanceStamped` | `shapely.Polygon` | `geo:wktLiteral` |
| `builtin_interfaces/Time` | UTC `datetime.datetime` | `xsd:dateTime` |
| `std_msgs/Float32`, `Float64` | `float` | `xsd:double` |
| `std_msgs/Int8`, `Int16`, `Int32`, `Int64` | `int` | `xsd:integer` |
| `std_msgs/UInt8`, `UInt16`, `UInt32`, `UInt64` | `int` | `xsd:integer` |
| `std_msgs/Char` | `str` | `xsd:string` |
| `std_msgs/Byte` | `int` | `xsd:integer` |
| `std_msgs/Bool` | `bool` | `xsd:boolean` |
| `std_msgs/String` | `str` | `xsd:string` |
| Python `float`, `int`, `str`, `bool` | Unchanged | Corresponding XSD datatype |
| Shapely geometry | Unchanged | `geo:wktLiteral` |

Geometry conversion has intentionally narrow semantics:

- A pose stores its position, not its orientation.
- A vector is represented as a WKT point.
- Polygon conversion uses the X and Y coordinates of each point.
- A query-time TF function stores only the transform translation.

Use a dedicated RDF model when orientation, frame IDs, covariance, or other message metadata must be preserved.

## Python API

`to_rdf_literal` unwraps a recognized ROS message and returns a `pyoxigraph.Literal`:

```python
from geometry_msgs.msg import Point32
from triplestar_core.conversions import to_rdf_literal

literal = to_rdf_literal(Point32(x=1.0, y=2.0, z=0.0))
```

Unknown values return `None` rather than being serialized as an arbitrary string. The reverse path converts a PyOxigraph literal to the Python value produced by RDFLib:

```python
from triplestar_core.conversions import rdf_literal_to_python

value = rdf_literal_to_python(literal)
```

A `geo:wktLiteral` becomes a Shapely geometry because the conversion module registers the RDFLib datatype binding at import time.

## In insertion templates

The subscription manager exposes conversion as the Jinja2 `rdf` filter:

```jinja2
INSERT DATA {
  <http://example.org/robot> <http://example.org/position>
    {{ msg.pose.position | rdf }} .
}
```

The filter output is an RDF term ready to embed in the rendered SPARQL update. Keep delimiters and punctuation in the template, not in the ROS message value.

## In extension functions

Literal arguments to a [custom function](../bringup/custom-functions.md) are converted to Python before the decorated function runs. Supported Python return values take the forward path back to an RDF literal.
