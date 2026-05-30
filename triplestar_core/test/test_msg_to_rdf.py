from builtin_interfaces.msg import Time as ROSTime
from geometry_msgs.msg import Point32
from geometry_msgs.msg import Polygon
from geometry_msgs.msg import PolygonInstance
from geometry_msgs.msg import PolygonInstanceStamped
from geometry_msgs.msg import PolygonStamped
from oxrdflib._converter import from_ox
import pyoxigraph as ox
import rdflib
from shapely import Point as ShapelyPoint
from shapely import wkt as shapely_wkt
from std_msgs.msg import Bool
from std_msgs.msg import Float32
from std_msgs.msg import Int32
from std_msgs.msg import String
from triplestar_core.conversions import to_rdf_literal

GEO = 'http://www.opengis.net/ont/geosparql#'
XSD = 'http://www.w3.org/2001/XMLSchema#'

# ── ROS → RDF forward conversion ──────────────────────────────────────


def test_point32_conversion():
    pt = Point32(x=1.0, y=2.0, z=0.0)
    literal = to_rdf_literal(pt)
    assert isinstance(literal, ox.Literal)
    assert literal.datatype == ox.NamedNode(GEO + 'wktLiteral')
    assert literal.value.startswith('POINT')


def test_polygon_conversion():
    poly = Polygon()
    poly.points = [
        Point32(x=1.0, y=2.0, z=0.0),
        Point32(x=3.0, y=4.0, z=0.0),
        Point32(x=5.0, y=6.0, z=0.0),
        Point32(x=1.0, y=2.0, z=0.0),
    ]
    literal = to_rdf_literal(poly)
    assert isinstance(literal, ox.Literal)
    assert literal.datatype == ox.NamedNode(GEO + 'wktLiteral')
    assert literal.value.startswith('POLYGON')


def test_float_conversion():
    literal = to_rdf_literal(3.14)
    assert isinstance(literal, ox.Literal)
    assert literal.datatype == ox.NamedNode(XSD + 'double')
    assert literal.value == '3.14'


def test_int_conversion():
    literal = to_rdf_literal(42)
    assert isinstance(literal, ox.Literal)
    assert literal.datatype == ox.NamedNode(XSD + 'integer')
    assert literal.value == '42'


def test_str_conversion():
    literal = to_rdf_literal('hello')
    assert isinstance(literal, ox.Literal)
    assert literal.datatype == ox.NamedNode(XSD + 'string')
    assert literal.value == 'hello'


def test_bool_conversion():
    literal = to_rdf_literal(True)
    assert isinstance(literal, ox.Literal)
    assert literal.datatype == ox.NamedNode(XSD + 'boolean')
    assert literal.value == 'true'


def test_none_conversion():
    literal = to_rdf_literal(None)
    assert literal is None


def test_unknown_type_conversion():
    assert to_rdf_literal([1, 2, 3]) is None


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

    assert to_rdf_literal(my_polygon_msg) is not None
    assert to_rdf_literal(my_polygon_msg_stamped) is not None
    assert to_rdf_literal(my_polygon_instance) is not None
    assert to_rdf_literal(my_polygon_instance_stamped) is not None


def test_time_conversion():
    t = ROSTime(sec=1238124124, nanosec=3029456453)
    assert to_rdf_literal(t) is not None


def test_std_msgs_float_conversion():
    float_msg = Float32(data=42.42)
    literal = to_rdf_literal(float_msg)
    assert isinstance(literal, ox.Literal)
    assert literal.datatype == ox.NamedNode(XSD + 'double')
    assert literal.value == '42.42'


def test_std_msgs_int_conversion():
    int_msg = Int32(data=42)
    literal = to_rdf_literal(int_msg)
    assert isinstance(literal, ox.Literal)
    assert literal.datatype == ox.NamedNode(XSD + 'integer')
    assert literal.value == '42'


def test_std_msgs_bool_conversion():
    bool_msg = Bool(data=True)
    literal = to_rdf_literal(bool_msg)
    assert isinstance(literal, ox.Literal)
    assert literal.datatype == ox.NamedNode(XSD + 'boolean')
    assert literal.value == 'true'


def test_std_msgs_string_conversion():
    string_msg = String(data='Hello, world!')
    literal = to_rdf_literal(string_msg)
    assert isinstance(literal, ox.Literal)
    assert literal.datatype == ox.NamedNode(XSD + 'string')
    assert literal.value == 'Hello, world!'


# ── Shapely → RDF (forward via rdflib lexicalizer) ────────────────────


def test_shapely_point_conversion():
    pt = ShapelyPoint(1.0, 2.0)
    literal = to_rdf_literal(pt)
    assert isinstance(literal, ox.Literal)
    assert literal.datatype == ox.NamedNode(GEO + 'wktLiteral')
    assert literal.value.startswith('POINT')


def test_shapely_polygon_conversion():
    from shapely import Polygon as ShapelyPolygon

    poly = ShapelyPolygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    literal = to_rdf_literal(poly)
    assert isinstance(literal, ox.Literal)
    assert literal.datatype == ox.NamedNode(GEO + 'wktLiteral')
    assert literal.value.startswith('POLYGON')


# ── RDF → Python (reverse via rdflib constructor) ─────────────────────


def test_reverse_wkt_to_shapely_point():
    """from_ox → .value should return a Shapely geometry via the registered constructor."""
    pyox = ox.Literal(
        'POINT (3 4)',
        datatype=ox.NamedNode(GEO + 'wktLiteral'),
    )
    rdf = from_ox(pyox)
    assert isinstance(rdf, rdflib.Literal)
    geom = rdf.value
    assert isinstance(geom, ShapelyPoint)
    assert geom.x == 3.0
    assert geom.y == 4.0


def test_reverse_wkt_to_shapely_polygon():
    """Round-trip a polygon through RDF and back."""
    from shapely import Polygon as ShapelyPolygon

    original = ShapelyPolygon([(0, 0), (1, 0), (1, 1), (0, 1)])

    # forward
    pyox = to_rdf_literal(original)

    # reverse
    rdf = from_ox(pyox)
    assert isinstance(rdf, rdflib.Literal)
    restored = rdf.value
    assert isinstance(restored, ShapelyPolygon)
    assert restored.equals(original)


def test_reverse_wkt_roundtrip():
    """Full round-trip: ROS Polygon → RDF → Shapely Polygon."""
    ros_poly = Polygon()
    ros_poly.points = [
        Point32(x=0, y=0),
        Point32(x=2, y=0),
        Point32(x=2, y=2),
        Point32(x=0, y=0),
    ]

    # ROS Polygon → RDF literal
    literal = to_rdf_literal(ros_poly)

    # RDF literal → Shapely geometry via registered constructor
    rdf = from_ox(literal)
    assert isinstance(rdf, rdflib.Literal)
    geom = rdf.value
    assert geom.area == 2.0
    assert geom.wkt == shapely_wkt.loads(geom.wkt).wkt  # valid WKT


def test_reverse_float_roundtrip():
    pyox = ox.Literal('3.14', datatype=ox.NamedNode(XSD + 'double'))
    rdf = from_ox(pyox)
    assert isinstance(rdf, rdflib.Literal)
    assert rdf.value == 3.14


def test_reverse_int_roundtrip():
    pyox = ox.Literal('42', datatype=ox.NamedNode(XSD + 'integer'))
    rdf = from_ox(pyox)
    assert isinstance(rdf, rdflib.Literal)
    assert rdf.value == 42
