from collections.abc import Callable
from inspect import signature
import logging
from pathlib import Path

from oxrdflib._converter import from_ox
from oxrdflib._converter import to_ox
from pyoxigraph import DefaultGraph
from pyoxigraph import NamedNode
from pyoxigraph import Quad
from pyoxigraph import QueryResultsFormat
from pyoxigraph import RdfFormat
from pyoxigraph import Store
import reasonable


class TriplestarKnowledgeBase:
    def __init__(
        self,
        store_path: Path | None,
        base_iri: str = 'http://example.org/',
        logger=None,
    ):

        self.logger = logger.get_child('KBInterface') if logger else logging.getLogger(__name__)

        self.store_path = store_path
        self.store: Store = Store(store_path)
        self.logger.info(
            f'Initialized store at {self.store_path if self.store_path else "in-memory"}'
        )

        self.base_iri = base_iri
        self.function_uri_base: str = f'{self.base_iri}/functions/'
        self.query_time_uri_base: str = f'{self.base_iri}/query-time/'
        self.reasoned_graph = NamedNode(f'{self.base_iri}/reasoned-graph')

        self.fn_registry: dict[NamedNode, Callable] = {}

        self.extra_iris = {
            'fn': self.function_uri_base,
            'qt': self.query_time_uri_base,
            '': self.base_iri,
        }

    def _add_function(self, name: str, function: Callable, prefix: str):
        uri = NamedNode(f'{self.extra_iris[prefix]}{name}')
        self.fn_registry[uri] = function
        params = ', '.join(signature(function).parameters.keys())
        self.logger.info(f'Registered {uri}, call in SPARQL via {prefix}:{name}({params})')

    def add_kb_function(self, name: str, function: Callable):
        self._add_function(name, function, 'fn')

    def add_query_time_function(self, name: str, function: Callable):
        self._add_function(name, function, 'qt')

    def run_reasoning(self):
        self.logger.info('Running reasoning...')

        self.reasoner = reasonable.PyReasoner()  # type:ignore

        # filter out RDF* triples (reasoner does not support RDF*)
        def is_plain_triple(t):
            return isinstance(t, tuple) and not any(isinstance(term, tuple) for term in t)

        triples = [
            from_ox(q.triple)
            for q in self.store.quads_for_pattern(None, None, None, DefaultGraph())
            if is_plain_triple(from_ox(q.triple))
        ]

        self.reasoner.update_graph(triples)
        inferred_quads = [
            Quad(to_ox(s), to_ox(p), to_ox(o), self.reasoned_graph)  # type: ignore
            for s, p, o in self.reasoner.reason()
        ]

        # refresh reasoned graph
        self.store.clear_graph(self.reasoned_graph)
        self.store.bulk_extend(inferred_quads)

    def load_files(self, file_paths: list[Path], file_format: RdfFormat = RdfFormat.TURTLE) -> int:
        loaded = 0
        for f in file_paths:
            try:
                with f.open('r', encoding='utf-8') as fh:
                    self.store.load(
                        input=fh,
                        format=file_format,
                        base_iri=self.base_iri,
                    )
                loaded += 1
            except Exception as e:
                self.logger.error(f'Failed to load {f}: {e}')
        self.logger.info(f'Loaded {loaded}/{len(file_paths)} files')
        return loaded

    def update(self, query: str) -> None:
        self.logger.debug(f'Executing update: {query}')
        try:
            self.store.update(
                query,
                base_iri=self.base_iri,
                prefixes=self.extra_iris,
                custom_functions=self.fn_registry,
            )
        except Exception as e:
            self.logger.error(f'Update failed: {e}')

    def query_json(self, query: str, reasoning: bool = False) -> str:
        self.logger.debug(f'Executing query: {query}')

        if reasoning:
            self.run_reasoning()
        try:
            result = self.store.query(
                query,
                base_iri=self.base_iri,
                prefixes=self.extra_iris,
                custom_functions=self.fn_registry,
                use_default_graph_as_union=reasoning,
            )
            return result.serialize(format=QueryResultsFormat.JSON).decode('utf-8')  # type: ignore
        except Exception as e:
            self.logger.error(f'Query execution failed: {e}')
            return ''

    def count_triples(self) -> int:
        return len(list(self.store.quads_for_pattern(None, None, None, None)))

    def clear(self):
        """Clear all data from the store. Returns True if successful."""
        self.store.clear()

    def optimize(self):
        if self.store_path:
            self.store.optimize()
