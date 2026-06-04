"""
Tests for triplestar_core.conversions.

Covers:
  • Forward:   Python / ROS / Shapely → RDF literal       (to_rdf_literal)
  • Reverse:   RDF literal → Python / Shapely geometry    (rdf_literal_to_python)
  • Roundtrip: ROS → RDF → Python
  • String → pyoxigraph term                              (string_to_oxi_term)
"""

from datetime import datetime
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
import pyoxigraph as ox
import pytest
import rdflib
from rdflib.namespace import GEO
from rdflib.namespace import XSD
from shapely import Point as ShapelyPoint
from shapely import Polygon as ShapelyPolygon
from shapely import wkt as shapely_wkt
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
from triplestar_core.conversions import rdf_literal_to_python
from triplestar_core.conversions import string_to_oxi_term
from triplestar_core.conversions import to_rdf_literal

# =============================================================================
#  Helpers
# =============================================================================


def _make_literal(value: str, datatype: str) -> ox.Literal:
    return ox.Literal(value, datatype=ox.NamedNode(datatype))


def _assert_literal(
    literal: Any,  # noqa: ANN401
    *,
    datatype: str,
    value: str,
) -> None:
    """Assert *literal* is an ``ox.Literal`` with the expected datatype and value."""
    assert isinstance(literal, ox.Literal)
    assert literal.datatype == ox.NamedNode(datatype)
    assert literal.value == value


def _assert_wkt_literal(literal: Any, *, wkt_prefix: str) -> None:  # noqa: ANN401
    """Assert *literal* is a ``geo:wktLiteral`` starting with *wkt_prefix*."""
    assert isinstance(literal, ox.Literal)
    assert literal.datatype == ox.NamedNode(GEO.wktLiteral)
    assert literal.value.startswith(wkt_prefix)


def _make_polygon() -> Polygon:
    poly = Polygon()
    poly.points = [
        Point32(x=0.0, y=0.0),
        Point32(x=2.0, y=0.0),
        Point32(x=2.0, y=2.0),
        Point32(x=0.0, y=0.0),
    ]
    return poly


# =============================================================================
#  Forward: Python builtins → RDF
# =============================================================================


class TestBuiltinForward:
    """Direct Python scalars sent through ``to_rdf_literal``."""

    @pytest.mark.parametrize(
        ('value', 'expected_type', 'expected_str'),
        [
            pytest.param(3.14, XSD.double, '3.14', id='float'),
            pytest.param(42, XSD.integer, '42', id='int'),
            pytest.param('hello', XSD.string, 'hello', id='str'),
            pytest.param(True, XSD.boolean, 'true', id='bool'),
            pytest.param('', XSD.string, '', id='empty string'),
        ],
    )
    def test_scalar(self, value: Any, expected_type: str, expected_str: str) -> None:  # noqa: ANN401
        _assert_literal(to_rdf_literal(value), datatype=expected_type, value=expected_str)

    def test_none(self) -> None:
        assert to_rdf_literal(None) is None

    def test_unknown_type(self) -> None:
        assert to_rdf_literal([1, 2, 3]) is None


# =============================================================================
#  Forward: ROS geometry messages → RDF
# =============================================================================


class TestGeometryForward:
    """ROS geometry messages unwrapped to Shapely then serialised as WKT."""

    @pytest.mark.parametrize(
        'msg',
        [
            pytest.param(Point32(x=1.0, y=2.0, z=0.0), id='Point32'),
            pytest.param(Point(x=1.0, y=2.0, z=0.0), id='Point'),
            pytest.param(Vector3(x=7.0, y=8.0, z=9.0), id='Vector3'),
        ],
    )
    def test_point_msgs(self, msg: Any) -> None:  # noqa: ANN401
        _assert_wkt_literal(to_rdf_literal(msg), wkt_prefix='POINT')

    def test_point_stamped(self) -> None:
        msg = PointStamped()
        msg.point.x, msg.point.y, msg.point.z = 1.0, 2.0, 3.0
        _assert_wkt_literal(to_rdf_literal(msg), wkt_prefix='POINT')

    def test_pose(self) -> None:
        msg = Pose()
        msg.position.x, msg.position.y, msg.position.z = 4.0, 5.0, 6.0
        _assert_wkt_literal(to_rdf_literal(msg), wkt_prefix='POINT')

    def test_vector3_stamped(self) -> None:
        msg = Vector3Stamped()
        msg.vector.x, msg.vector.y, msg.vector.z = 1.0, 2.0, 3.0
        _assert_wkt_literal(to_rdf_literal(msg), wkt_prefix='POINT')

    @pytest.mark.parametrize(
        'msg',
        [
            pytest.param(_make_polygon(), id='Polygon'),
            pytest.param(
                lambda: setattr(m := PolygonStamped(), 'polygon', _make_polygon()) or m,
                id='PolygonStamped',
            ),
        ],
    )
    def test_polygon_msgs(self, msg: Any) -> None:  # noqa: ANN401
        _assert_wkt_literal(to_rdf_literal(msg() if callable(msg) else msg), wkt_prefix='POLYGON')

    def test_polygon_instance(self) -> None:
        msg = PolygonInstance()
        msg.polygon = _make_polygon()
        _assert_wkt_literal(to_rdf_literal(msg), wkt_prefix='POLYGON')

    def test_polygon_instance_stamped(self) -> None:
        msg = PolygonInstanceStamped()
        msg.polygon.polygon = _make_polygon()
        _assert_wkt_literal(to_rdf_literal(msg), wkt_prefix='POLYGON')

    def test_polygon_no_points(self) -> None:
        _assert_wkt_literal(to_rdf_literal(Polygon()), wkt_prefix='POLYGON')

    def test_time(self) -> None:
        literal = to_rdf_literal(ROSTime(sec=1_238_124_124, nanosec=3_029_456_453))
        assert isinstance(literal, ox.Literal)
        assert literal.datatype == ox.NamedNode(XSD.dateTime)
        assert literal.value is not None


# =============================================================================
#  Forward: ROS std_msgs → RDF
# =============================================================================


class TestStdMsgsForward:
    """ROS std message wrappers unwrapped via ``.data`` then serialised."""

    @pytest.mark.parametrize(
        ('msg_cls', 'data', 'expected_type', 'expected_str'),
        [
            pytest.param(Float32, 42.42, XSD.double, '42.42', id='Float32'),
            pytest.param(Float64, 42.42, XSD.double, '42.42', id='Float64'),
            pytest.param(Int8, 42, XSD.integer, '42', id='Int8'),
            pytest.param(Int16, 42, XSD.integer, '42', id='Int16'),
            pytest.param(Int32, 42, XSD.integer, '42', id='Int32'),
            pytest.param(Int64, 42, XSD.integer, '42', id='Int64'),
            pytest.param(UInt8, 42, XSD.integer, '42', id='UInt8'),
            pytest.param(UInt16, 42, XSD.integer, '42', id='UInt16'),
            pytest.param(UInt32, 42, XSD.integer, '42', id='UInt32'),
            pytest.param(UInt64, 42, XSD.integer, '42', id='UInt64'),
            pytest.param(Bool, True, XSD.boolean, 'true', id='Bool'),
            pytest.param(Char, 'A', XSD.string, 'A', id='Char'),
            pytest.param(Byte, 42, XSD.integer, '42', id='Byte'),
            pytest.param(String, 'Hello, world!', XSD.string, 'Hello, world!', id='String'),
            pytest.param(String, '', XSD.string, '', id='String empty'),
        ],
    )
    def test_std_msg(
        self,
        msg_cls: type,
        data: Any,  # noqa: ANN401
        expected_type: str,
        expected_str: str,
    ) -> None:
        _assert_literal(
            to_rdf_literal(msg_cls(data=data)),
            datatype=expected_type,
            value=expected_str,
        )


# =============================================================================
#  Forward: Shapely → RDF
# =============================================================================


class TestShapelyForward:
    """Shapely geometries serialised via the ``rdflib`` WKT lexicalizer."""

    def test_point(self) -> None:
        _assert_wkt_literal(to_rdf_literal(ShapelyPoint(1.0, 2.0)), wkt_prefix='POINT')

    def test_polygon(self) -> None:
        _assert_wkt_literal(
            to_rdf_literal(ShapelyPolygon([(0, 0), (1, 0), (1, 1), (0, 1)])),
            wkt_prefix='POLYGON',
        )

    def test_empty_geometry(self) -> None:
        _assert_wkt_literal(to_rdf_literal(ShapelyPolygon()), wkt_prefix='POLYGON')


# =============================================================================
#  Reverse: rdf_literal_to_python (public API)
# =============================================================================


class TestRdfLiteralToPython:
    """The public ``rdf_literal_to_python`` function."""

    @pytest.mark.parametrize(
        ('value', 'datatype', 'expected_type', 'expected'),
        [
            pytest.param('hello', XSD.string, str, 'hello', id='string'),
            pytest.param('42', XSD.integer, int, 42, id='integer'),
            pytest.param('3.14', XSD.double, float, 3.14, id='double'),
            pytest.param('true', XSD.boolean, bool, True, id='boolean true'),
            pytest.param('false', XSD.boolean, bool, False, id='boolean false'),
        ],
    )
    def test_scalar(
        self,
        value: str,
        datatype: str,
        expected_type: type,
        expected: Any,  # noqa: ANN401
    ) -> None:
        result = rdf_literal_to_python(_make_literal(value, datatype))
        assert isinstance(result, expected_type)
        assert result == expected

    def test_wkt_point(self) -> None:
        result = rdf_literal_to_python(_make_literal('POINT (5 6)', GEO.wktLiteral))
        assert isinstance(result, ShapelyPoint)
        assert result.x == 5.0
        assert result.y == 6.0

    def test_datetime(self) -> None:
        result = rdf_literal_to_python(_make_literal('2024-01-15T10:30:00Z', XSD.dateTime))
        assert isinstance(result, datetime)

    def test_none_input_raises(self) -> None:
        with pytest.raises(Exception):
            none_val: Any = None  # noqa: ANN401
            rdf_literal_to_python(none_val)

    def test_wkt_roundtrip_rdflib_layer(self) -> None:
        """``from_ox`` → ``rdflib.Literal.value`` produces a Shapely geometry."""
        original = ShapelyPolygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        rdf_lit = from_ox(to_rdf_literal(original))
        assert isinstance(rdf_lit, rdflib.Literal)
        assert isinstance(rdf_lit.value, ShapelyPolygon)
        assert rdf_lit.value.equals(original)


# =============================================================================
#  Roundtrip: ROS → RDF → Python
# =============================================================================


class TestRoundtrip:
    """Full end-to-end roundtrip through the public API."""

    @pytest.mark.parametrize(
        ('msg', 'expected_type', 'check'),
        [
            pytest.param(Float32(data=3.14), float, lambda v: v == 3.14, id='Float32'),
            pytest.param(Int32(data=42), int, lambda v: v == 42, id='Int32'),
            pytest.param(Bool(data=True), bool, lambda v: v is True, id='Bool'),
            pytest.param(String(data='roundtrip'), str, lambda v: v == 'roundtrip', id='String'),
        ],
    )
    def test_scalar_roundtrip(self, msg: Any, expected_type: type, check: Any) -> None:  # noqa: ANN401
        literal = to_rdf_literal(msg)
        assert isinstance(literal, ox.Literal)
        result = rdf_literal_to_python(literal)
        assert isinstance(result, expected_type)
        assert check(result)

    def test_point32(self) -> None:
        literal = to_rdf_literal(Point32(x=7.0, y=8.0, z=0.0))
        assert isinstance(literal, ox.Literal)
        geom = rdf_literal_to_python(literal)
        assert isinstance(geom, ShapelyPoint)
        assert geom.x == 7.0
        assert geom.y == 8.0

    def test_polygon(self) -> None:
        literal = to_rdf_literal(_make_polygon())
        assert isinstance(literal, ox.Literal)
        geom = rdf_literal_to_python(literal)
        assert isinstance(geom, ShapelyGeometry)
        assert geom.area == 2.0
        assert geom.wkt == shapely_wkt.loads(geom.wkt).wkt


# =============================================================================
#  string_to_oxi_term
# =============================================================================


class TestStringToOxiTerm:
    """Parsing N3-formatted strings into pyoxigraph terms."""

    # -- IRIs -------------------------------------------------------------

    @pytest.mark.parametrize(
        ('valid_iri', 'expected'),
        [
            pytest.param(
                'xsd:integer',
                ox.NamedNode('http://www.w3.org/2001/XMLSchema#integer'),
                id='known prefixed IRI',
            ),
            pytest.param(
                '<http://example.org/x>', ox.NamedNode('http://example.org/x'), id='absolute IRI'
            ),
        ],
    )
    def test_valid_iri(self, valid_iri: str, expected: ox.NamedNode) -> None:
        term = string_to_oxi_term(valid_iri)
        assert isinstance(term, ox.NamedNode)
        assert term == expected

    @pytest.mark.parametrize(
        'invalid_iri',
        [
            pytest.param('<foo/bar>', id='relative IRI'),
            pytest.param('http://example.org/x', id='bare (no angle brackets)'),
            # pytest.param('xsd:integer', id='prefixed name'),
            pytest.param('random:integer', id='unknown prefix'),
        ],
    )
    def test_invalid_iri_rejected(self, invalid_iri: str) -> None:
        with pytest.raises(Exception):
            string_to_oxi_term(invalid_iri)

    # -- Literals ---------------------------------------------------------

    @pytest.mark.parametrize(
        ('input_str', 'expected_value', 'expected_datatype', 'expected_lang'),
        [
            pytest.param('"hello"', 'hello', XSD.string, None, id='plain literal'),
            pytest.param(
                '"42"^^<http://www.w3.org/2001/XMLSchema#integer>',
                '42',
                XSD.integer,
                None,
                id='integer',
            ),
            pytest.param(
                '"3.14"^^<http://www.w3.org/2001/XMLSchema#double>',
                '3.14',
                XSD.double,
                None,
                id='double',
            ),
            pytest.param(
                '"true"^^<http://www.w3.org/2001/XMLSchema#boolean>',
                'true',
                XSD.boolean,
                None,
                id='boolean explicit iri',
            ),
            pytest.param('"true"^^xsd:boolean', 'true', XSD.boolean, None, id='boolean xsd prefix'),
            pytest.param('"hello"@en', 'hello', None, 'en', id='language tagged'),
        ],
    )
    def test_literal(
        self,
        input_str: str,
        expected_value: str,
        expected_datatype: str | None,
        expected_lang: str | None,
    ) -> None:
        term = string_to_oxi_term(input_str)
        assert isinstance(term, ox.Literal)
        assert term.value == expected_value
        if expected_datatype is not None:
            assert term.datatype == ox.NamedNode(expected_datatype)
        if expected_lang is not None:
            assert term.language == expected_lang

    def test_literal_with_escaped_quotes(self) -> None:
        """Literals containing escaped double-quotes are parsed correctly."""
        term = string_to_oxi_term('"say \\"hello\\""')
        assert isinstance(term, ox.Literal)
        assert '\\"' not in term.value
        assert 'hello' in term.value

    # -- Rejected ---------------------------------------------------------

    @pytest.mark.parametrize(
        'invalid_input',
        [
            pytest.param('', id='empty string'),
            pytest.param('_:foo', id='blank node'),
            pytest.param('hello', id='unquoted literal'),
        ],
    )
    def test_invalid_rejected(self, invalid_input: str) -> None:
        with pytest.raises(ValueError):
            string_to_oxi_term(invalid_input)
