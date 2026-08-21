import json
from pathlib import Path

from pyoxigraph import Literal
from pyoxigraph import NamedNode
from pyoxigraph import Quad
import pytest
from triplestar_core.knowledge_base import TriplestarKnowledgeBase

# =============================================================================
#  Fixtures
# =============================================================================

_DATA_DIR = Path(__file__).parent / 'data'


@pytest.fixture
def kb():
    """KB with no data."""
    return TriplestarKnowledgeBase(store_path=None, base_iri='http://example.org')


@pytest.fixture
def kb_with_data(kb):
    kb.load_files(list(_DATA_DIR.glob('*.ttl')))
    return kb


@pytest.fixture
def kb_obj_room_scenario(kb):
    kb.load_files([_DATA_DIR / 'obj_room.ttl'])
    return kb


@pytest.fixture
def kb_with_triple(kb):
    """KB containing a single IRI triple: ex:x ex:p ex:y."""
    kb.store.add(
        Quad(
            NamedNode('http://example.org/x'),
            NamedNode('http://example.org/p'),
            NamedNode('http://example.org/y'),
        )
    )
    return kb


@pytest.fixture
def kb_with_literal(kb):
    """KB containing a single literal triple: ex:x ex:p 5."""
    kb.store.add(
        Quad(
            NamedNode('http://example.org/x'),
            NamedNode('http://example.org/p'),
            Literal(5),
        )
    )
    return kb


# =============================================================================
#  ASK queries
# =============================================================================


class TestAskQuery:
    def test_returns_false_on_empty_store(self, kb) -> None:
        assert (
            kb.query(
                'ASK { <http://example.org/x> <http://example.org/p> <http://example.org/y> }'
            )
            is False
        )

    def test_returns_true_when_triple_exists(self, kb_with_triple) -> None:
        assert (
            kb_with_triple.query(
                'ASK { <http://example.org/x> <http://example.org/p> <http://example.org/y> }'
            )
            is True
        )


# =============================================================================
#  SELECT queries
# =============================================================================


class TestSelectQuery:
    def test_returns_json_string(self, kb_with_data) -> None:
        result = kb_with_data.query('SELECT ?s WHERE { ?s ?p ?o }')
        assert isinstance(result, str)
        parsed = json.loads(result)
        assert 'results' in parsed


# =============================================================================
#  Substitutions
# =============================================================================


class TestSubstitutions:
    @pytest.mark.parametrize(
        ('substitution', 'expected'),
        [
            pytest.param({'s': '<http://example.org/x>'}, True, id='matching subject'),
            pytest.param({'s': '<http://example.org/y>'}, False, id='non-matching subject'),
        ],
    )
    def test_iri_substitution(self, kb_with_triple, substitution, expected) -> None:
        result = kb_with_triple.query('ASK { ?s ?p ?o }', substitutions=substitution)
        assert result is expected

    @pytest.mark.parametrize(
        ('term', 'expected'),
        [
            pytest.param(
                '"4"^^<http://www.w3.org/2001/XMLSchema#integer>', False, id='non-matching integer'
            ),
            pytest.param(
                '"5"^^<http://www.w3.org/2001/XMLSchema#integer>', True, id='matching integer'
            ),
            pytest.param('"5"^^xsd:integer', True, id='matching integer with prefix'),
            pytest.param('"5"', False, id='plain string does not match integer'),
        ],
    )
    def test_literal_substitution(self, kb_with_literal, term, expected) -> None:
        result = kb_with_literal.query('ASK WHERE { ?s ?p ?o }', substitutions={'o': term})
        assert result is expected

    def test_invalid_substitution_raises(self, kb: TriplestarKnowledgeBase) -> None:
        with pytest.raises(Exception):  # noqa: B017
            kb.query(
                'ASK { ?s <http://example.org/p> <http://example.org/y> }',
                substitutions={'s': 'http://example.org/x'},
            )

    @pytest.mark.parametrize(
        ('room', 'min_height', 'expected_count', 'expected_obj'),
        [
            pytest.param(
                '<http://example.org/room1>',
                '"5"^^<http://www.w3.org/2001/XMLSchema#integer>',
                1,
                'http://example.org/obj1',
                id='obj1 in room1, height 10 > 5',
            ),
            pytest.param(
                '<http://example.org/room1>',
                '"15"^^<http://www.w3.org/2001/XMLSchema#integer>',
                0,
                None,
                id='obj1 in room1, height 10 not > 15',
            ),
            pytest.param(
                '<http://example.org/room2>',
                '"5"^^<http://www.w3.org/2001/XMLSchema#integer>',
                0,
                None,
                id='no objects in room2',
            ),
        ],
    )
    def test_filter_substitution(
        self, kb_obj_room_scenario, room, min_height, expected_count, expected_obj
    ) -> None:

        # Parametrized query: find objects in a given room taller than a given min height
        select_query = """
            SELECT ?obj ?min_height ?height ?room WHERE {
                ?obj <http://example.org/height> ?obj_height .
                ?obj <http://example.org/locatedIn> ?room .
                FILTER(?obj_height > ?min_height)
            }
        """
        result = kb_obj_room_scenario.query(
            select_query,
            substitutions={
                'room': room,
                'min_height': min_height,
            },
        )
        assert isinstance(result, str)
        parsed = json.loads(result)
        bindings = parsed['results']['bindings']
        assert len(bindings) == expected_count
        if expected_obj is not None:
            assert bindings[0]['obj']['value'] == expected_obj
