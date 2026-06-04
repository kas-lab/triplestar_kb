from pathlib import Path

from triplestar_core.config import QueryServiceConfig
from triplestar_core.config import QueryServicesConfig
from triplestar_core.kb_lifecycle_node import TriplestarKBNode
from triplestar_core.knowledge_base import TriplestarKnowledgeBase
from triplestar_core.query_services.query_service import FileQueryService


class QueryServiceManager:
    def __init__(
        self,
        node,
        config: QueryServicesConfig,
        kb: TriplestarKnowledgeBase,
        queries_dir: Path,
    ):
        self.logger = node.get_logger().get_child('query_service_manager')
        self.query_services: dict[str, FileQueryService] = {}

        for name, srv_config in config.query_services.items():
            service = self._create_service(node, kb, queries_dir, name, srv_config)
            if service:
                self.query_services[name] = service

        self.logger.info(f'Initialized — services: {list(self.query_services.keys())}')

    def _create_service(
        self,
        node: TriplestarKBNode,
        kb: TriplestarKnowledgeBase,
        queries_dir: Path,
        name: str,
        srv_config: QueryServiceConfig,
    ) -> FileQueryService | None:
        try:
            if not srv_config.query_file:
                raise KeyError(f'No query_file specified for service "{name}"')

            query_file = queries_dir / srv_config.query_file
            if not query_file.exists():
                raise FileNotFoundError(f'Query file not found: {query_file}')

            reasoning = srv_config.reasoning

            return FileQueryService(
                node=node,
                name=name,
                query_file=query_file,
                query_fn=lambda q, s, r=reasoning: kb.query(q, reasoning=r, substitutions=s),
            )
        except (KeyError, ValueError, FileNotFoundError, RuntimeError) as e:
            self.logger.error(f'Failed to create query service "{name}": {e}')
            return None
