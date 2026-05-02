from pathlib import Path

from triplestar_core.knowledge_base import TriplestarKnowledgeBase
from triplestar_core.query_services.query_service import FileQueryService


class QueryServiceManager:
    def __init__(self, node, config: dict, kb: TriplestarKnowledgeBase, queries_dir: Path):
        self.node = node
        self.logger = node.get_logger().get_child('query_service_manager')

        self.query_services: dict[str, FileQueryService] = {}

        for name, srv_config in config.items():
            try:
                query_file_name = srv_config.get('query_file')
                if not query_file_name:
                    raise KeyError(f'No query_file specified for service "{name}"')

                query_file = queries_dir / query_file_name
                if not query_file.exists():
                    raise FileNotFoundError(f'Query file not found: {query_file}')

                reasoning_enabled = srv_config.get('reasoning', False)

                def query_fn(sparql: str, reasoning=reasoning_enabled) -> str:
                    return kb.query_json(sparql, reasoning=reasoning)

                self.query_services[name] = FileQueryService(
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
