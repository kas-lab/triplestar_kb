from pathlib import Path
from typing import Callable, List

import reasonable
from oxrdflib._converter import from_ox, to_ox
from pyoxigraph import DefaultGraph, NamedNode, Quad, QueryResultsFormat, RdfFormat, Store


class TriplestarKnowledgeBase:
    def __init__(
        self,
        store_path: Path,
        base_iri: str,
        logger,
    ):
        if logger is None:
            raise ValueError('logger must be provided')
        self.logger = logger.get_child('KBInterface')

        self.store_path = store_path
        self.store: Store = Store(store_path)
        self.logger.info(
            f'Initialized store at {self.store_path if self.store_path else "in-memory"}'
        )

        self.base_iri = base_iri
        self.reasoned_graph = NamedNode(f'{self.base_iri}/reasoned-graph')
        self.function_uri_base: str = f'{self.base_iri}/functions/'
        self.query_time_uri_base: str = f'{self.base_iri}/query-time/'
        self.extra_iris = {
            'fn': self.function_uri_base,
            'qt': self.query_time_uri_base,
        }

        self.custom_functions: dict[NamedNode, Callable] = {}

    def _add_function(self, name: str, base_uri: str, function: Callable):
        uri = NamedNode(f'{base_uri}{name}')
        self.custom_functions[uri] = function
        self.logger.info(f'Added custom function for {uri.value}()')

    def add_custom_function(self, name: str, function: Callable):
        self._add_function(name, self.function_uri_base, function)

    def add_query_time_function(self, name: str, function: Callable):
        self._add_function(name, self.query_time_uri_base, function)

    def run_reasoning(self):
        self.logger.info('Running reasoning...')
        # NOTE: Reasoner is recreated each time, is that OK?
        reasoner = reasonable.PyReasoner()

        # filter out RDF* triples (reasoner does not support RDF*)
        def is_plain_triple(t):
            return isinstance(t, tuple) and not any(isinstance(term, tuple) for term in t)

        triples = [
            from_ox(q.triple)
            for q in self.store.quads_for_pattern(None, None, None, DefaultGraph())
            if is_plain_triple(from_ox(q.triple))
        ]

        reasoner.from_graph(triples)
        inferred = reasoner.reason()

        inferred_quads = [
            Quad(to_ox(s), to_ox(p), to_ox(o), self.reasoned_graph)  # type: ignore
            for s, p, o in inferred
        ]

        # refresh reasoned graph
        self.store.remove_graph(self.reasoned_graph)
        self.store.extend(inferred_quads)

    def load_files(self, file_paths: List[Path], format: RdfFormat = RdfFormat.TURTLE) -> int:
        loaded = 0
        for f in file_paths:
            try:
                with f.open('r', encoding='utf-8') as fh:
                    self.store.load(input=fh, format=format)
                loaded += 1
            except Exception as e:
                self.logger.error(f'Failed to load {f}: {e}')
        self.logger.info(f'Loaded {loaded}/{len(file_paths)} files')
        return loaded

    def query_json(self, query: str, reasoning: bool = False) -> str:
        self.logger.debug(f'Executing query: {query}')

        if reasoning:
            self.run_reasoning()
        try:
            result = self.store.query(
                query,
                base_iri=self.base_iri,
                prefixes=self.extra_iris,
                custom_functions=self.custom_functions,
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
