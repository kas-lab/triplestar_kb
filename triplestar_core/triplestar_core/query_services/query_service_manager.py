from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from rclpy.lifecycle import LifecycleNode

from triplestar_core.config import QueryServiceConfig
from triplestar_core.config import QueryServicesConfig
from triplestar_core.knowledge_base import TriplestarKnowledgeBase
from triplestar_core.query_services.query_service import FileQueryService
from triplestar_msgs.srv import SPARQLQuery

if TYPE_CHECKING:
    from rclpy.service import Service


class QueryServiceManager:
    """
    Owns the SPARQL query service and any configured file-backed query services.

    Construct once in on_configure (cheap - just stores config/refs).
    start()/stop() from on_activate/on_deactivate actually register/tear
    down the ROS services.
    """

    def __init__(
        self,
        node: LifecycleNode,
        config: QueryServicesConfig,
        kb: TriplestarKnowledgeBase,
        queries_dir: Path,
    ):
        self.logger = node.get_logger().get_child('query_service_manager')
        self.config = config
        self.kb = kb
        self.node = node
        self.queries_dir = queries_dir

        # Only populated between start() and stop().
        self.query_service: Service | None = None
        self.file_query_services: dict[str, FileQueryService] = {}

    def start(self):
        for name, srv_config in self.config.query_services.items():
            service = self._create_file_query_service(
                self.node, self.kb, self.queries_dir, name, srv_config
            )
            if service:
                self.file_query_services[name] = service

        self.query_service = self.node.create_service(
            SPARQLQuery, '/triplestar/sparql', self.query_callback
        )

        self.logger.info(f'Started — services: {list(self.file_query_services.keys())}')

    def stop(self):
        if self.query_service is not None:
            self.node.destroy_service(self.query_service)
            self.query_service = None

        for service in self.file_query_services.values():
            service.destroy()

        self.file_query_services = {}

        self.logger.info('Stopped')

    def query_callback(
        self, request: SPARQLQuery.Request, response: SPARQLQuery.Response
    ) -> SPARQLQuery.Response:
        """Handle a SPARQL query request."""
        self.logger.debug(f'Received query request: {request}')

        response.result = self.kb.query(request.query, reasoning=request.reasoning)
        response.success = response.result != ''
        return response

    def _create_file_query_service(
        self,
        node: LifecycleNode,
        kb: TriplestarKnowledgeBase,
        queries_dir: Path,
        name: str,
        srv_config: QueryServiceConfig,
    ) -> FileQueryService | None:
        try:
            if not srv_config.query_file:
                raise ValueError(f'No query_file specified for service "{name}"')

            query_file = queries_dir / srv_config.query_file
            if not query_file.exists():
                raise FileNotFoundError(f'Query file not found: {query_file}')

            return FileQueryService(
                node=node,
                name=name,
                query_file=query_file,
                query_fn=lambda q, reasoning, s: kb.query(q, reasoning=reasoning, substitutions=s),
            )
        except (ValueError, FileNotFoundError, RuntimeError) as e:
            self.logger.error(f'Failed to create query service "{name}": {e}')
            return None
