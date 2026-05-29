from datetime import datetime
from datetime import timezone
from typing import Any
from typing import Callable
from typing import Dict
from typing import Optional
from typing import Type

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
from pyoxigraph import Literal as RdfLiteral
from pyoxigraph import NamedNode
from shapely import Point as ShapelyPoint
from shapely import Polygon as ShapelyPolygon
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

GEO = 'http://www.opengis.net/ont/geosparql#'
XSD = 'http://www.w3.org/2001/XMLSchema#'


class RosToRdfLiteralConverterRegistry:
    def __init__(self):
        self._converters: Dict[Type, Callable[[Any], RdfLiteral]] = {}

    def register(self, *types: Type):
        def decorator(func: Callable[[Any], RdfLiteral]):
            for t in types:
                self._converters[t] = func
            return func

        return decorator

    def convert(self, value: Any) -> Optional[RdfLiteral]:
        if value is None:
            return None
        value_type = type(value)
        # Exact-type lookup first — avoids any subclass ordering dependency.
        if value_type in self._converters:
            return self._converters[value_type](value)
        # Fall back to subclass match for types registered as base classes.
        for t, func in self._converters.items():
            if issubclass(value_type, t):
                return func(value)
        return None


registry = RosToRdfLiteralConverterRegistry()


@registry.register(Point, Point32, PointStamped)
def convert_point(point) -> RdfLiteral:
    if hasattr(point, 'point'):
        point = point.point
    shapely_point = ShapelyPoint(point.x, point.y, point.z)
    return RdfLiteral(shapely_point.wkt, datatype=NamedNode(GEO + 'wktLiteral'))


@registry.register(Pose)
def convert_pose(pose) -> RdfLiteral:
    shapely_point = ShapelyPoint(pose.position.x, pose.position.y, pose.position.z)
    return RdfLiteral(shapely_point.wkt, datatype=NamedNode(GEO + 'wktLiteral'))


@registry.register(Vector3, Vector3Stamped)
def convert_vector3(vector) -> RdfLiteral:
    if hasattr(vector, 'vector'):
        vector = vector.vector
    shapely_point = ShapelyPoint(vector.x, vector.y, vector.z)
    return RdfLiteral(shapely_point.wkt, datatype=NamedNode(GEO + 'wktLiteral'))


@registry.register(Polygon, PolygonStamped, PolygonInstance, PolygonInstanceStamped)
def convert_polygon(polygon) -> RdfLiteral:
    if hasattr(polygon, 'polygon'):
        polygon = polygon.polygon
    coords = [(p.x, p.y) for p in polygon.points] if polygon.points else []
    shp = ShapelyPolygon(coords)
    return RdfLiteral(shp.wkt, datatype=NamedNode(GEO + 'wktLiteral'))


@_registry.register(ShapelyGeometry)
def convert_shapely_geometry(geom: ShapelyGeometry) -> RdfLiteral:
    """Convert any Shapely geometry to a WKT literal."""
    return RdfLiteral(geom.wkt, datatype=NamedNode(GEO + 'wktLiteral'))

@registry.register(ROSTime)
def convert_time(ros_time: ROSTime) -> RdfLiteral:
    dt = datetime.fromtimestamp(ros_time.sec + ros_time.nanosec / 1e9, tz=timezone.utc)
    return RdfLiteral(
        dt.isoformat().replace('+00:00', 'Z'),
        datatype=NamedNode(XSD + 'dateTime'),
    )


@registry.register(float)
def convert_float(value: float) -> RdfLiteral:
    return RdfLiteral(str(value), datatype=NamedNode(XSD + 'float'))


@registry.register(bool)
def convert_bool(value: bool) -> RdfLiteral:
    return RdfLiteral(str(value).lower(), datatype=NamedNode(XSD + 'boolean'))


@registry.register(int)
def convert_int(value: int) -> RdfLiteral:
    return RdfLiteral(str(value), datatype=NamedNode(XSD + 'integer'))


@registry.register(str)
def convert_str(value: str) -> RdfLiteral:
    return RdfLiteral(value, datatype=NamedNode(XSD + 'string'))


@registry.register(
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
def ros_msg_to_literal(msg: Any) -> Optional[RdfLiteral]:
    return registry.convert(msg)
