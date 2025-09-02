from pathlib import Path
from typing import Callable, List, Optional

from pyoxigraph import NamedNode, QueryResultsFormat, RdfFormat, Store


class TriplestarKBInterface:
    def __init__(self, store_path: Optional[Path] = None, logger=None):
        self.logger = logger.get_child("KBInterface") if logger else logger
        self.logger.info("Initializing TriplestarKBInterface")
        self.store_path = store_path
        self._initialize_store()
        self.custom_functions: dict[NamedNode, Callable] = {}

    def _add_custom_function(self, function_uri: NamedNode, function: Callable):
        self.custom_functions[function_uri] = function
        self.logger.info(f"Added custom function for {function_uri}")

    def _initialize_store(self):
        if self.store_path is None:
            self.store = Store()
            self.logger.info("Initialized in-memory store")
        else:
            try:
                if self.store_path.exists():
                    self.logger.info(
                        f"Store path {self.store_path} already exists. Loading existing store."
                    )
                else:
                    self.logger.info(
                        f"Store path {self.store_path} does not exist. Creating new store."
                    )
                self.store = Store(self.store_path)
                self.logger.info(f"Initialized store at {self.store_path}")
                self.logger.info(f"{self.count_triples()} triples present")
            except Exception as e:
                self.logger.error(
                    f"Failed to initialize store at {self.store_path}: {e}"
                )
                raise

    def load_file(self, file_path: Path, format: RdfFormat = RdfFormat.TURTLE) -> bool:
        """
        Load a single RDF file into the store.

        Args:
            file_path: Path to the RDF file
            format: RDF format (defaults to Turtle)

        Returns:
            True if successful, False otherwise
        """
        if self.store is None:
            raise RuntimeError("Store not initialized")

        if not file_path.exists():
            return False

        try:
            with file_path.open("r", encoding="utf-8") as file:
                self.store.load(input=file, format=format)
            self.logger.info(f"Loaded file {file_path}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to load file {file_path}: {e}")
            return False

    def load_files(
        self, file_paths: List[Path], format: RdfFormat = RdfFormat.TURTLE
    ) -> int:
        """
        Load multiple RDF files into the store.

        Args:
            file_paths: List of paths to RDF files
            format: RDF format (defaults to Turtle)

        Returns:
            Number of files successfully loaded
        """
        loaded_count = 0
        failed_count = 0
        for file_path in file_paths:
            if self.load_file(file_path, format):
                loaded_count += 1
            else:
                failed_count += 1
        self.logger.info(
            f"Loaded {loaded_count} files, failed to load {failed_count} files"
        )
        return loaded_count

    def count_triples(self) -> int:
        """
        Count total number of triples in the store.

        Returns:
            Number of triples
        """
        if self.store is None:
            raise RuntimeError("Store not initialized")

        try:
            query = "SELECT (COUNT(*) AS ?count) WHERE {?s ?p ?o}"
            result = next(self.store.query(query))
            return result["count"].value
        except Exception:
            return 0

    def clear(self) -> bool:
        """
        Clear all data from the store.

        Returns:
            True if successful, False otherwise
        """
        if not self.store:
            raise RuntimeError("Store not initialized")

        try:
            # Clear all named graphs and the default graph
            self.store.update("CLEAR ALL")
            return True
        except Exception:
            return False

    def close(self) -> None:
        """Close and cleanup the store."""
        if self.store:
            if self.store_path is not None:
                self.optimize()
            self.store = None

    def optimize(self) -> bool:
        """
        Optimize the store.

        Returns:
            True if successful, False otherwise
        """
        if not self.store:
            raise RuntimeError("Store not initialized")

        try:
            self.store.optimize()
            return True
        except OSError as e:
            self.logger.error(
                f"Failed to optimize store: {e}",
            )
            return False

    def query_json(self, query: str) -> str:
        """
        Execute a SPARQL query on the store and return results as a JSON string.

        Args:
            query: The SPARQL query string to execute.

        Returns:
            A JSON string containing the query results, or an empty string if an error occurs.
        """
        if not self.store:
            raise RuntimeError("Store not initialized")

        self.logger.info(f"Executing query: {query}")

        try:
            result = self.store.query(query, custom_functions=self.custom_functions)
            result_json = result.serialize(format=QueryResultsFormat.JSON)
            return result_json.decode("utf-8")
        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")

        return ""
