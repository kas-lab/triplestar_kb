from pathlib import Path
from typing import Callable, List, Optional

import reasonable
from oxrdflib._converter import from_ox, to_ox
from pyoxigraph import DefaultGraph, NamedNode, Quad, QueryResultsFormat, RdfFormat, Store


class TriplestarKBInterface:
    def __init__(self, store_path: Optional[Path] = None, logger=None):
        if logger is None:
            raise ValueError('logger must be provided')
        self.logger = logger.get_child('KBInterface')

        self.store_path = store_path
        self.store: Store = Store(store_path) if store_path else Store()
        self.logger.info(
            f'Initialized store at {self.store_path if self.store_path else "in-memory"}'
        )

        self.reasoned_graph = NamedNode('http://example.org/reasoned-graph')

        self.custom_functions: dict[NamedNode, Callable] = {}

    def add_custom_function(self, function_uri: NamedNode, function: Callable):
        self.custom_functions[function_uri] = function
        self.logger.info(f'Added custom function for {function_uri.value}()')

    def run_reasoning(self):
        self.logger.info('Running reasoning...')
        # NOTE: Reasoner is recreated each time, is that OK?
        reasoner = reasonable.PyReasoner()

        quads = self.store.quads_for_pattern(None, None, None, DefaultGraph())
        triples = [from_ox(q.triple) for q in quads]

        # NOTE: remove RDF* triples (maybe we can skip this later when reasonable catches up?)
        triples = [t for t in triples if not any(isinstance(term, tuple) for term in t)]  # type: ignore[arg-type]

        reasoner.from_graph(triples)
        inferred = reasoner.reason()

        def triple_to_reasoned_quad(s, p, o):
            return Quad(to_ox(s), to_ox(p), to_ox(o), self.reasoned_graph)  # type: ignore[arg-type]

        inferred_quads = [triple_to_reasoned_quad(s, p, o) for s, p, o in inferred]

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
                query, custom_functions=self.custom_functions, use_default_graph_as_union=reasoning
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
