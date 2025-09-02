from builtin_interfaces.msg import Time as ROSTime
from geometry_msgs.msg import (
    Point32,
    Polygon,
    PolygonInstance,
    PolygonInstanceStamped,
    PolygonStamped,
)
from pyoxigraph import Literal as RdfLiteral
from pyoxigraph import NamedNode
from sensor_msgs.msg import Temperature
from std_msgs.msg import Bool, Float32, Int32, String

from triplestar_kb.msg_to_rdf import ros_msg_to_literal

GEO = "http://www.opengis.net/ont/geosparql#"
XSD = "http://www.w3.org/2001/XMLSchema#"


def test_point32_conversion():
    pt = Point32(x=1.0, y=2.0, z=0.0)
    literal = ros_msg_to_literal(pt)
    assert isinstance(literal, RdfLiteral)
    assert literal.datatype == NamedNode(GEO + "wktLiteral")
    assert literal.value.startswith("POINT")


def test_polygon_conversion():
    poly = Polygon()
    poly.points = [
        Point32(x=1.0, y=2.0, z=0.0),
        Point32(x=3.0, y=4.0, z=0.0),
        Point32(x=5.0, y=6.0, z=0.0),
        Point32(x=1.0, y=2.0, z=0.0),
    ]
    literal = ros_msg_to_literal(poly)
    assert isinstance(literal, RdfLiteral)
    assert literal.datatype == NamedNode(GEO + "wktLiteral")
    assert literal.value.startswith("POLYGON")


def test_float_conversion():
    literal = ros_msg_to_literal(3.14)
    assert isinstance(literal, RdfLiteral)
    assert literal.datatype == NamedNode(XSD + "float")
    assert literal.value == "3.14"


def test_ros_msg_to_literal_polygon():
    my_polygon_msg = Polygon()
    my_polygon_msg.points = [
        Point32(x=1.0, y=2.0, z=0.0),
        Point32(x=3.0, y=4.0, z=0.0),
        Point32(x=5.0, y=6.0, z=0.0),
    ]

    my_polygon_msg_stamped = PolygonStamped()
    my_polygon_msg_stamped.polygon = my_polygon_msg
    my_polygon_instance = PolygonInstance()
    my_polygon_instance.polygon = my_polygon_msg
    my_polygon_instance_stamped = PolygonInstanceStamped()
    my_polygon_instance_stamped.polygon = my_polygon_instance

    assert ros_msg_to_literal(my_polygon_msg) is not None
    assert ros_msg_to_literal(my_polygon_msg_stamped) is not None
    assert ros_msg_to_literal(my_polygon_instance) is not None
    assert ros_msg_to_literal(my_polygon_instance_stamped) is not None


def test_ros_msg_to_literal_temperature():
    temperature_msg = Temperature()
    temperature_msg.temperature = 36.6
    temperature_msg.variance = 0.5

    assert ros_msg_to_literal(temperature_msg, "temperature") == RdfLiteral(
        "36.6", datatype=NamedNode(XSD + "float")
    )
    assert ros_msg_to_literal(temperature_msg, "variance") == RdfLiteral(
        "0.5", datatype=NamedNode(XSD + "float")
    )


def test_nonexistent_field_returns_none():
    msg = Temperature()
    assert ros_msg_to_literal(msg, "non_existent_field") is None


def test_ros_msg_to_literal_time():
    t = ROSTime(sec=1238124124, nanosec=3029456453)
    assert ros_msg_to_literal(t) is not None


def test_std_msgs_float_conversion():
    float_msg = Float32(data=42.42)
    literal = ros_msg_to_literal(float_msg)
    assert isinstance(literal, RdfLiteral)
    assert literal.datatype == NamedNode(XSD + "float")
    assert literal.value == "42.42"


def test_std_msgs_int_conversion():
    int_msg = Int32(data=42)
    literal = ros_msg_to_literal(int_msg)
    assert isinstance(literal, RdfLiteral)
    assert literal.datatype == NamedNode(XSD + "integer")
    assert literal.value == "42"


def test_std_msgs_bool_conversion():
    bool_msg = Bool(data=True)
    literal = ros_msg_to_literal(bool_msg)
    assert isinstance(literal, RdfLiteral)
    assert literal.datatype == NamedNode(XSD + "boolean")
    assert literal.value == "true"


def test_std_msgs_string_conversion():
    string_msg = String(data="Hello, world!")
    literal = ros_msg_to_literal(string_msg)
    assert isinstance(literal, RdfLiteral)
    assert literal.datatype == NamedNode(XSD + "string")
    assert literal.value == "Hello, world!"
