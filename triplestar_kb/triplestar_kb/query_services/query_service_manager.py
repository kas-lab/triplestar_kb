from pathlib import Path
from typing import Callable

from triplestar_kb.kb_interface import TriplestarKBInterface
from triplestar_kb.query_services.query_service import QueryService


class QueryServiceManager:
    def __init__(self, node, config: dict, kb: TriplestarKBInterface, queries_dir: Path):
        self.node = node
        self.logger = node.get_logger().get_child('query_service_manager')

        self.query_services: dict[str, QueryService] = {}

        query_fn = self._make_query_fn(kb)

        for name, svc_cfg in config.items():
            try:
                query_file = self._resolve_query_file(svc_cfg, queries_dir, name)
                self.query_services[name] = QueryService(
                    node=node,
                    name=name,
                    query_file=query_file,
                    query_fn=query_fn,
                )
            except (KeyError, ValueError, FileNotFoundError, RuntimeError) as e:
                self.logger.error(f'Failed to create query service "{name}": {e}')

        self.logger.info(
            f'QueryServiceManager initialized — query services: {list(self.query_services.keys())}'
        )

    def _make_query_fn(self, kb: TriplestarKBInterface) -> Callable[[str], str]:
        def query_fn(sparql: str) -> str:
            if kb.store is None:
                self.logger.error('KB store is not initialized, dropping query')
                return ''
            return kb.query_json(sparql)

        return query_fn

    def _resolve_query_file(self, cfg: dict, queries_dir: Path, name: str) -> Path:
        if 'query_file' not in cfg or not cfg['query_file']:
            raise KeyError(f'Missing required field "query_file" in query service "{name}"')

        query_file = queries_dir / cfg['query_file']

        if not query_file.exists():
            raise FileNotFoundError(f'Query file not found: {query_file}')

        return query_file
