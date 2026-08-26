from collections.abc import Callable
from inspect import signature
import logging
from pathlib import Path

from opentelemetry import trace
from oxrdflib._converter import from_ox
from oxrdflib._converter import to_ox
from pyoxigraph import BlankNode
from pyoxigraph import DefaultGraph
from pyoxigraph import Literal
from pyoxigraph import NamedNode
from pyoxigraph import Quad
from pyoxigraph import QueryBoolean
from pyoxigraph import QueryResultsFormat
from pyoxigraph import QuerySolutions
from pyoxigraph import RdfFormat
from pyoxigraph import Store
from pyoxigraph import Variable
import reasonable

from triplestar_core.conversions import string_to_oxi_term

TRACER = trace.get_tracer('triplestar_bench')


class KnowledgeBase:
    def __init__(
        self,
        store_path: Path | None,
        base_iri: str = 'http://example.org/',
        logger=None,
    ):

        self.logger = logger.get_child('kb') if logger else logging.getLogger(__name__)

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

        self.reasoner = reasonable.PyReasoner()  # type:ignore

    def _add_function(self, name: str, function: Callable, prefix: str):
        uri = NamedNode(f'{self.extra_iris[prefix]}{name}')
        self.fn_registry[uri] = function
        params = ', '.join(signature(function).parameters.keys())
        self.logger.info(f'Registered {uri}, call in SPARQL via {prefix}:{name}({params})')

    def _remove_function(self, name: str, prefix: str):
        uri = NamedNode(f'{self.extra_iris[prefix]}{name}')
        if uri in self.fn_registry:
            del self.fn_registry[uri]
            self.logger.info(f'Removed function: {uri}')
        else:
            self.logger.warning(f'No function found: {uri}')

    def add_kb_function(self, name: str, function: Callable):
        self._add_function(name, function, 'fn')

    def remove_kb_function(self, name: str):
        self._remove_function(name, 'fn')

    def add_query_time_function(self, name: str, function: Callable):
        self._add_function(name, function, 'qt')

    def remove_query_time_function(self, name: str):
        self._remove_function(name, 'qt')

    @TRACER.start_as_current_span('run_reasoning')
    def run_reasoning(self):
        self.logger.info('Running reasoning...')

        # filter out RDF* triples (reasoner does not support RDF*)
        def is_plain_triple(t):
            return isinstance(t, tuple) and not any(isinstance(term, tuple) for term in t)

        with TRACER.start_as_current_span('fetch_base_triples') as span:
            triples = []
            for q in self.store.quads_for_pattern(None, None, None, DefaultGraph()):
                t = from_ox(q.triple)
                if is_plain_triple(t):
                    triples.append(t)
            base_set = set(triples)
            span.set_attribute('triples_count', len(triples))

        with TRACER.start_as_current_span('update_reasoner_graph'):
            self.reasoner.update_graph(triples)

        with TRACER.start_as_current_span('reason'):
            reasoned = self.reasoner.reason()

        with TRACER.start_as_current_span('filter_inferred_triples'):
            inferred_quads = [
                Quad(to_ox(s), to_ox(p), to_ox(o), self.reasoned_graph)  # type: ignore
                for s, p, o in reasoned
                if (s, p, o) not in base_set
            ]

        with TRACER.start_as_current_span('refresh_reasoned_graph'):
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
            except OSError as e:
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
            raise

    @staticmethod
    def make_substitutions(
        bindings: dict[str, str],
    ) -> dict[Variable, NamedNode | Literal | BlankNode]:
        return {Variable(k): string_to_oxi_term(v) for k, v in bindings.items()}

    def query(
        self,
        query: str,
        reasoning: bool = False,
        substitutions: dict[str, str] | None = None,
    ) -> str | bool | None:
        """
        Execute a SPARQL query and return the results.

        For SELECT queries, returns a JSON string of the results.
        For ASK queries, returns a boolean.
        """
        self.logger.debug(f'Executing query: {query}')

        oxi_substitutions = self.make_substitutions(substitutions) if substitutions else None

        if reasoning:
            self.run_reasoning()

        try:
            result = self.store.query(
                query,
                base_iri=self.base_iri,
                prefixes=self.extra_iris,
                custom_functions=self.fn_registry,
                use_default_graph_as_union=reasoning,
                substitutions=oxi_substitutions,  # type: ignore
            )
            if isinstance(result, QueryBoolean):
                return bool(result)
            elif isinstance(result, QuerySolutions):
                return result.serialize(format=QueryResultsFormat.JSON).decode('utf-8')  # ty:ignore[unresolved-attribute]
            raise ValueError('CONSTRUCT and DESCRIBE queries are not supported in TriplestarKB')

        except (ValueError, OSError, RuntimeError) as e:
            self.logger.error(f'Query execution failed: {e}')
            return None

    def count_triples(self) -> int:
        return len(list(self.store.quads_for_pattern(None, None, None, None)))

    def clear(self):
        """Clear all data from the store. Returns True if successful."""
        self.store.clear()

    def optimize(self):
        if self.store_path:
            self.store.optimize()
