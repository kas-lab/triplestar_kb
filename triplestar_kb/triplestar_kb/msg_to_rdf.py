from typing import Any, Callable, Dict, Optional, Type

from builtin_interfaces.msg import Time as ROSTime
from geometry_msgs.msg import (
    Point,
    Point32,
    PointStamped,
    Polygon,
    PolygonInstance,
    PolygonInstanceStamped,
    PolygonStamped,
    Pose,
    Vector3,
    Vector3Stamped,
)
from pyoxigraph import Literal as RdfLiteral
from pyoxigraph import NamedNode
from shapely import Point as ShapelyPoint
from shapely import Polygon as ShapelyPolygon
from std_msgs.msg import (
    Bool,
    Byte,
    Char,
    Float32,
    Float64,
    Int8,
    Int16,
    Int32,
    Int64,
    String,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
)

GEO = "http://www.opengis.net/ont/geosparql#"
XSD = "http://www.w3.org/2001/XMLSchema#"
RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"


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
        for t in self._converters:
            if issubclass(value_type, t):
                return self._converters[t](value)
        return None


registry = RosToRdfLiteralConverterRegistry()


@registry.register(Point, Point32, PointStamped)
def convert_point(point) -> RdfLiteral:
    if hasattr(point, "header"):
        point = point.point
    shapely_point = ShapelyPoint(point.x, point.y)
    return RdfLiteral(shapely_point.wkt, datatype=NamedNode(GEO + "wktLiteral"))


@registry.register(Pose)
def convert_pose(pose) -> RdfLiteral:
    shapely_point = ShapelyPoint(pose.position.x, pose.position.y)
    return RdfLiteral(shapely_point.wkt, datatype=NamedNode(GEO + "wktLiteral"))


@registry.register(Vector3, Vector3Stamped)
def convert_vector3(vector) -> RdfLiteral:
    shapely_point = ShapelyPoint(vector.x, vector.y)
    return RdfLiteral(shapely_point.wkt, datatype=NamedNode(GEO + "wktLiteral"))


@registry.register(Polygon, PolygonStamped, PolygonInstance, PolygonInstanceStamped)
def convert_polygon(polygon) -> RdfLiteral:
    if hasattr(polygon, "header"):
        polygon = polygon.polygon
    if hasattr(polygon, "id"):
        polygon = polygon.polygon
    if not polygon.points:
        shp = ShapelyPolygon()
    else:
        coords = [(p.x, p.y) for p in polygon.points]
        shp = ShapelyPolygon(coords)
    return RdfLiteral(shp.wkt, datatype=NamedNode(GEO + "wktLiteral"))


@registry.register(ROSTime)
def convert_time(ros_time: ROSTime) -> RdfLiteral:
    from datetime import datetime, timezone

    dt = datetime.fromtimestamp(ros_time.sec + ros_time.nanosec / 1e9, tz=timezone.utc)
    return RdfLiteral(
        dt.isoformat().replace("+00:00", "Z"), datatype=NamedNode(XSD + "dateTime")
    )


# Built in datatypes
@registry.register(float)
def convert_float(value: float) -> RdfLiteral:
    datatype = NamedNode(XSD + "float")
    return RdfLiteral(str(value), datatype=datatype)


# must come before int, because bool is a subclass of int
@registry.register(bool)
def convert_bool(value: bool) -> RdfLiteral:
    datatype = NamedNode(XSD + "boolean")
    return RdfLiteral(str(value).lower(), datatype=datatype)


@registry.register(int)
def convert_int(value: int) -> RdfLiteral:
    datatype = NamedNode(XSD + "integer")
    return RdfLiteral(str(value), datatype=datatype)


@registry.register(str)
def convert_str(value: str) -> RdfLiteral:
    datatype = NamedNode(XSD + "string")
    return RdfLiteral(value, datatype=datatype)


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
def convert_std_msg(value) -> RdfLiteral | None:
    return registry.convert(value.data)


def ros_msg_to_literal(msg: Any, field: Optional[str] = None) -> Optional[RdfLiteral]:
    if field:
        value = getattr(msg, field, None)
        if value is None:
            return None
    else:
        value = msg
    return registry.convert(value)
