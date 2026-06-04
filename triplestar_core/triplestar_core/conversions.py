from datetime import datetime
from datetime import timezone
from typing import Any

from builtin_interfaces.msg import Time as ROSTime
from geometry_msgs.msg import Point
from geometry_msgs.msg import Point32
from geometry_msgs.msg import PointStamped
from geometry_msgs.msg import Polygon
from geometry_msgs.msg import PolygonInstance
from geometry_msgs.msg import PolygonInstanceStamped
from geometry_msgs.msg import PolygonStamped
from geometry_msgs.msg import Pose
from geometry_msgs.msg import Vector3
from geometry_msgs.msg import Vector3Stamped
from oxrdflib._converter import from_ox
from oxrdflib._converter import to_ox
import pyoxigraph as ox
import rdflib
from rdflib import Literal
from rdflib.namespace import GEO
from rdflib.term import Node as RdflibNode
from rdflib.util import from_n3
from shapely import Point as ShapelyPoint
from shapely import Polygon as ShapelyPolygon
from shapely import wkt as _shapely_wkt
from shapely.geometry.base import BaseGeometry as ShapelyGeometry
from std_msgs.msg import Bool
from std_msgs.msg import Byte
from std_msgs.msg import Char
from std_msgs.msg import Float32
from std_msgs.msg import Float64
from std_msgs.msg import Int8
from std_msgs.msg import Int16
from std_msgs.msg import Int32
from std_msgs.msg import Int64
from std_msgs.msg import String
from std_msgs.msg import UInt8
from std_msgs.msg import UInt16
from std_msgs.msg import UInt32
from std_msgs.msg import UInt64

_STD_MSGS = (
    Float32,
    Float64,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Char,
    Byte,
    Bool,
    String,
)

_ROS_TO_PYTHON = {
    # std messages
    **{t: lambda m: m.data for t in _STD_MSGS},
    # geometry_msgs → Shapely
    Point: lambda p: ShapelyPoint(p.x, p.y, p.z),
    Point32: lambda p: ShapelyPoint(p.x, p.y, p.z),
    PointStamped: lambda p: ShapelyPoint(p.point.x, p.point.y, p.point.z),
    Pose: lambda p: ShapelyPoint(p.position.x, p.position.y, p.position.z),
    Vector3: lambda v: ShapelyPoint(v.x, v.y, v.z),
    Vector3Stamped: lambda v: ShapelyPoint(v.vector.x, v.vector.y, v.vector.z),
    Polygon: lambda poly: ShapelyPolygon([(p.x, p.y) for p in poly.points]),
    PolygonStamped: lambda poly: ShapelyPolygon([(p.x, p.y) for p in poly.polygon.points]),
    PolygonInstance: lambda poly: ShapelyPolygon([(p.x, p.y) for p in poly.polygon.points]),
    PolygonInstanceStamped: lambda poly: ShapelyPolygon(
        [(p.x, p.y) for p in poly.polygon.polygon.points]
    ),
    ROSTime: lambda t: datetime.fromtimestamp(t.sec + t.nanosec / 1e9, tz=timezone.utc),
}

# bind shapely objects to wkt literals
rdflib.term.bind(
    GEO.wktLiteral,
    ShapelyGeometry,
    constructor=lambda s: _shapely_wkt.loads(s),
    lexicalizer=lambda geom: geom.wkt,
)


def to_rdf_literal(msg) -> ox.Literal | None:
    """
    Convert a ROS message or python value to a pyoxigraph RDF literal.

    * ROS messages are unwrapped via ``_ROS_TO_PYTHON``
    * Everything else is passed to ``rdflib.Literal()`` which uses
      registered lexicalizers (shapely → ``geo:wktLiteral``) and
      built-in type inference (``float`` → ``xsd:double``, …)

    Returns ``None`` when the type is not recognised.
    """
    if msg is None:
        return None

    converter = _ROS_TO_PYTHON.get(type(msg))
    value = converter(msg) if converter else msg

    result = to_ox(Literal(value))
    if not isinstance(result, ox.Literal):
        return None

    # rdflib falls back to xsd:string for unknown types — reject those
    # unless the original value was already a string.
    if (
        not isinstance(value, (str, bytes))
        and result.datatype.value == 'http://www.w3.org/2001/XMLSchema#string'
    ):
        return None

    return result


def rdf_literal_to_python(literal: ox.Literal) -> Any:
    """Convert a pyoxigraph RDF literal to a Python value."""
    rdflib_literal = from_ox(literal)
    if not isinstance(rdflib_literal, rdflib.Literal):
        raise TypeError(
            f'Expected rdflib.Literal after conversion, got {type(rdflib_literal).__name__}'
        )
    return rdflib_literal.value


def string_to_oxi_term(string: str) -> ox.NamedNode | ox.Literal | ox.BlankNode:
    if not string.startswith('<') and not string.startswith('"'):
        raise ValueError(f'Invalid RDF term (must be <uri> or "literal"...): {string}')

    term = from_n3(string)
    if not isinstance(term, RdflibNode):
        raise ValueError(f'Cannot parse {string} to an rdf term')
    ox_term = to_ox(term)
    if not isinstance(ox_term, (ox.NamedNode, ox.Literal)):
        raise ValueError(f'Cannot convert {string} to a IRI or Literal')
    return ox_term
