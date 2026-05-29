from builtin_interfaces.msg import Time as ROSTime
from geometry_msgs.msg import Point32
from geometry_msgs.msg import Polygon
from geometry_msgs.msg import PolygonInstance
from geometry_msgs.msg import PolygonInstanceStamped
from geometry_msgs.msg import PolygonStamped
from pyoxigraph import Literal as RdfLiteral
from pyoxigraph import NamedNode
from shapely import Point as ShapelyPoint
from std_msgs.msg import Bool
from std_msgs.msg import Float32
from std_msgs.msg import Int32
from std_msgs.msg import String
from triplestar_core.msg_to_rdf import _registry as registry

GEO = 'http://www.opengis.net/ont/geosparql#'
XSD = 'http://www.w3.org/2001/XMLSchema#'


def test_point32_conversion():
    pt = Point32(x=1.0, y=2.0, z=0.0)
    literal = registry.convert(pt)
    assert isinstance(literal, RdfLiteral)
    assert literal.datatype == NamedNode(GEO + 'wktLiteral')
    assert literal.value.startswith('POINT')


def test_polygon_conversion():
    poly = Polygon()
    poly.points = [
        Point32(x=1.0, y=2.0, z=0.0),
        Point32(x=3.0, y=4.0, z=0.0),
        Point32(x=5.0, y=6.0, z=0.0),
        Point32(x=1.0, y=2.0, z=0.0),
    ]
    literal = registry.convert(poly)
    assert isinstance(literal, RdfLiteral)
    assert literal.datatype == NamedNode(GEO + 'wktLiteral')
    assert literal.value.startswith('POLYGON')


def test_float_conversion():
    literal = registry.convert(3.14)
    assert isinstance(literal, RdfLiteral)
    assert literal.datatype == NamedNode(XSD + 'float')
    assert literal.value == '3.14'


def test_int_conversion():
    literal = registry.convert(42)
    assert isinstance(literal, RdfLiteral)
    assert literal.datatype == NamedNode(XSD + 'integer')
    assert literal.value == '42'


def test_str_conversion():
    literal = registry.convert('hello')
    assert isinstance(literal, RdfLiteral)
    assert literal.datatype == NamedNode(XSD + 'string')
    assert literal.value == 'hello'


def test_bool_conversion():
    literal = registry.convert(True)
    assert isinstance(literal, RdfLiteral)
    assert literal.datatype == NamedNode(XSD + 'boolean')
    assert literal.value == 'true'


def test_none_conversion():
    assert registry.convert(None) is None


def test_unknown_type_conversion():
    assert registry.convert([1, 2, 3]) is None


def test_polygon_variants():
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

    assert registry.convert(my_polygon_msg) is not None
    assert registry.convert(my_polygon_msg_stamped) is not None
    assert registry.convert(my_polygon_instance) is not None
    assert registry.convert(my_polygon_instance_stamped) is not None


def test_time_conversion():
    t = ROSTime(sec=1238124124, nanosec=3029456453)
    assert registry.convert(t) is not None


def test_std_msgs_float_conversion():
    float_msg = Float32(data=42.42)
    literal = registry.convert(float_msg)
    assert isinstance(literal, RdfLiteral)
    assert literal.datatype == NamedNode(XSD + 'float')
    assert literal.value == '42.42'


def test_std_msgs_int_conversion():
    int_msg = Int32(data=42)
    literal = registry.convert(int_msg)
    assert isinstance(literal, RdfLiteral)
    assert literal.datatype == NamedNode(XSD + 'integer')
    assert literal.value == '42'


def test_std_msgs_bool_conversion():
    bool_msg = Bool(data=True)
    literal = registry.convert(bool_msg)
    assert isinstance(literal, RdfLiteral)
    assert literal.datatype == NamedNode(XSD + 'boolean')
    assert literal.value == 'true'


def test_std_msgs_string_conversion():
    string_msg = String(data='Hello, world!')
    literal = registry.convert(string_msg)
    assert isinstance(literal, RdfLiteral)
    assert literal.datatype == NamedNode(XSD + 'string')
    assert literal.value == 'Hello, world!'


def test_shapely_point_conversion():
    pt = ShapelyPoint(1.0, 2.0)
    literal = registry.convert(pt)
    assert isinstance(literal, RdfLiteral)
    assert literal.datatype == NamedNode(GEO + 'wktLiteral')
    assert literal.value.startswith('POINT')


def test_shapely_geometry_via_base_class():
    from shapely import Polygon as ShapelyPolygon

    poly = ShapelyPolygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    literal = registry.convert(poly)
    assert isinstance(literal, RdfLiteral)
    assert literal.datatype == NamedNode(GEO + 'wktLiteral')
    assert literal.value.startswith('POLYGON')


def test_mapping_table_is_nonempty():
    table = registry.mapping_table()
    assert table.startswith('| Source type')
    assert 'convert_' in table
    assert 'BaseGeometry' in table
    assert 'Float32' in table
