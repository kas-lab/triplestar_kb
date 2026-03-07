from pathlib import Path
from typing import Callable, List, Optional

from pyoxigraph import NamedNode, QueryResultsFormat, RdfFormat, Store


class TriplestarKBInterface:
    def __init__(self, store_path: Optional[Path] = None, logger=None):
        if logger is None:
            raise ValueError('logger must be provided')
        self.logger = logger.get_child('KBInterface')
        self.store: Optional[Store] = None
        self.store_path = store_path
        self.custom_functions: dict[NamedNode, Callable] = {}
        self._initialize_store()

    def _add_custom_function(self, function_uri: NamedNode, function: Callable):
        self.custom_functions[function_uri] = function
        self.logger.info(f'Added custom function for {function_uri.value}()')

    def _initialize_store(self):
        if self.store_path is None:
            self.store = Store()
            self.logger.info('Initialized in-memory store')
        else:
            try:
                action = 'Loading existing' if self.store_path.exists() else 'Creating new'
                self.logger.info(f'{action} store at {self.store_path}')
                self.store = Store(self.store_path)
                self.logger.info(f'{self._count_triples()} triples present')
            except Exception as e:
                self.logger.error(f'Failed to initialize store at {self.store_path}: {e}')
                raise

    def load_file(self, file_path: Path, format: RdfFormat = RdfFormat.TURTLE) -> bool:
        """Load a single RDF file into the store. Returns True if successful."""
        if self.store is None:
            raise RuntimeError('Store not initialized')
        if not file_path.exists():
            return False
        try:
            with file_path.open('r', encoding='utf-8') as f:
                self.store.load(input=f, format=format)
            self.logger.info(f'Loaded {file_path}')
            return True
        except Exception as e:
            self.logger.error(f'Failed to load {file_path}: {e}')
            return False

    def load_files(self, file_paths: List[Path], format: RdfFormat = RdfFormat.TURTLE) -> int:
        """Load multiple RDF files. Returns the number of files successfully loaded."""
        loaded = sum(self.load_file(f, format) for f in file_paths)
        self.logger.info(f'Loaded {loaded}/{len(file_paths)} files')
        return loaded

    def _count_triples(self) -> int:
        """Return the total number of triples in the store."""
        assert self.store is not None
        try:
            query_result = self.store.query('SELECT (COUNT(*) AS ?count) WHERE {?s ?p ?o}')
            row = next(query_result)  # type: ignore[arg-type, call-overload]
            return int(row['count'].value)
        except Exception:
            return 0

    def clear(self) -> bool:
        """Clear all data from the store. Returns True if successful."""
        if self.store is None:
            raise RuntimeError('Store not initialized')
        try:
            self.store.update('CLEAR ALL')
            return True
        except Exception:
            return False

    def optimize(self) -> bool:
        """Optimize the store. Returns True if successful."""
        if self.store is None:
            raise RuntimeError('Store not initialized')
        try:
            self.store.optimize()
            return True
        except OSError as e:
            self.logger.error(f'Failed to optimize store: {e}')
            return False

    def close(self) -> None:
        """Optimize (if persistent) and release the store."""
        if self.store is not None:
            if self.store_path is not None:
                self.optimize()
            self.store = None

    def query_json(self, query: str) -> str:
        """
        Execute a SPARQL query and return the results as a JSON string.
        Returns an empty string if the query fails.
        """
        if self.store is None:
            raise RuntimeError('Store not initialized')

        self.logger.debug(f'Executing query: {query}')
        try:
            result = self.store.query(query, custom_functions=self.custom_functions)
            return result.serialize(format=QueryResultsFormat.JSON).decode('utf-8')  # type: ignore
        except Exception as e:
            self.logger.error(f'Query execution failed: {e}')
            return ''
