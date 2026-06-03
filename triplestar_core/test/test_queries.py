import json

from pyoxigraph import NamedNode
from pyoxigraph import Quad
from triplestar_core.knowledge_base import TriplestarKnowledgeBase


def test_ask_query_returns_bool():
    kb = TriplestarKnowledgeBase(store_path=None, base_iri='http://example.org')
    kb.store.add(
        Quad(
            NamedNode('http://example.org/x'),
            NamedNode('http://example.org/p'),
            NamedNode('http://example.org/y'),
        )
    )
    result = kb.query('ASK { <x> <p> <y> }')
    assert result is True


def test_select_query_returns_json():
    kb = TriplestarKnowledgeBase(None)
    result = kb.query('SELECT ?s WHERE { ?s ?p ?o }')
    assert isinstance(result, str)
    parsed = json.loads(result)
    assert 'results' in parsed


def test_substitution():
    kb = TriplestarKnowledgeBase(None)
    result = kb.query('ASK { ?s <p> <y> }', substitutions={'s': '<http://example.org/x>'})
    assert isinstance(result, bool)


def test_substitution_fail():
    kb = TriplestarKnowledgeBase(None)
    try:
        kb.query('ASK { ?s <p> <y> }', substitutions={'s': 'http://example.org/x'})
    except Exception:
        pass
    else:
        assert False, 'Expected an error for missing angle brackets in substitution'
