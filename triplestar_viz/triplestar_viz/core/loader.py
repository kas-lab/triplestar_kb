"""
RDF data loading utilities for visualization.

This module provides functions to load RDF data from various sources
(files, stores) and execute SPARQL queries.
"""

from pathlib import Path
from typing import Callable, Dict, Iterator, Optional, Union

from pyoxigraph import NamedNode, Quad, Store, Triple


class RDFLoader:
    """Utilities for loading RDF data from various sources."""

    @staticmethod
    def from_file(
        path: Union[str, Path],
        format: str = 'turtle',
    ) -> Store:
        """
        Load RDF data from a file into a new in-memory store.

        Args:
            path: Path to the RDF file
            format: RDF format (turtle, ntriples, rdfxml, etc.)

        Returns:
            A pyoxigraph Store containing the loaded data

        Example:
            >>> store = RDFLoader.from_file("data.ttl")
            >>> store = RDFLoader.from_file("data.nt", format="ntriples")
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f'File not found: {path}')

        # Map common format names to pyoxigraph MIME types
        format_map = {
            'turtle': 'text/turtle',
            'ttl': 'text/turtle',
            'ntriples': 'application/n-triples',
            'nt': 'application/n-triples',
            'rdfxml': 'application/rdf+xml',
            'xml': 'application/rdf+xml',
            'n3': 'text/n3',
            'nquads': 'application/n-quads',
            'nq': 'application/n-quads',
            'trig': 'application/trig',
        }

        # Get the MIME type for pyoxigraph
        mime_type = format_map.get(format.lower(), format)

        # Create in-memory store
        store = Store()

        # Read file content
        with open(path, 'rb') as f:
            data = f.read()

        # Parse and load into store
        store.load(data, mime_type)

        return store

    @staticmethod
    def from_store(store_path: Union[str, Path], read_only: bool = True) -> Store:
        """
        Open an existing pyoxigraph store.

        Args:
            store_path: Path to the store directory
            read_only: If True, open in read-only mode

        Returns:
            A pyoxigraph Store

        Example:
            >>> store = RDFLoader.from_store("/tmp/kb_store")
        """
        store_path = Path(store_path)
        if not store_path.exists():
            raise FileNotFoundError(f'Store not found: {store_path}')

        if read_only:
            return Store.read_only(str(store_path))
        else:
            return Store(str(store_path))

    @staticmethod
    def execute_query(
        store: Store,
        query: str,
        custom_functions: Optional[Dict[NamedNode, Callable]] = None,
    ) -> Iterator[Union[Quad, Triple]]:
        """
        Execute a SPARQL CONSTRUCT query on a store.

        Args:
            store: The RDF store to query
            query: SPARQL CONSTRUCT query string
            custom_functions: Optional dict of custom SPARQL functions

        Returns:
            Iterator of Quad or Triple objects

        Example:
            >>> query = "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"
            >>> results = RDFLoader.execute_query(store, query)
            >>> for triple in results:
            ...     print(triple)
        """
        if custom_functions:
            return store.query(query, custom_functions=custom_functions)
        else:
            return store.query(query)

    @staticmethod
    def load_query_from_file(path: Union[str, Path]) -> str:
        """
        Load a SPARQL query from a file.

        Args:
            path: Path to the query file

        Returns:
            Query string

        Example:
            >>> query = RDFLoader.load_query_from_file("query.rq")
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f'Query file not found: {path}')

        with open(path, 'r', encoding='utf-8') as f:
            return f.read()

    @staticmethod
    def get_all_quads(store: Store) -> Iterator[Quad]:
        """
        Get all quads from a store.

        Args:
            store: The RDF store

        Returns:
            Iterator of all quads in the store

        Example:
            >>> for quad in RDFLoader.get_all_quads(store):
            ...     print(quad)
        """
        return iter(store)
