import json
import re
from pathlib import Path
from typing import Callable, Literal

from rclpy.lifecycle import LifecycleNode
from rclpy.node import Node
from triplestar_kb_msgs.srv import AskQuery, SelectQuery

"""
The query service sets up a ROS service backed by a .sparql file.
On each call it reads the file and delegates execution to query_fn.
"""

QueryType = Literal['select', 'ask']


def _detect_query_type(query_file: Path) -> QueryType:
    """Detect the SPARQL query type by inspecting the file contents."""
    sparql = query_file.read_text()
    stripped = re.sub(r'#[^\n]*', '', sparql)  # strip line comments
    match = re.search(r'\b(SELECT|ASK)\b', stripped, re.IGNORECASE)
    if match is None:
        raise ValueError(
            f'Could not detect query type in "{query_file}" — expected a SELECT or ASK query'
        )
    return match.group(1).lower()  # type: ignore[return-value]


class QueryService:
    def __init__(
        self,
        node: Node | LifecycleNode,
        name: str,
        query_file: Path,
        query_fn: Callable[[str], str],
    ):
        self.name = name
        self.logger = node.get_logger().get_child(name)

        if not query_file.exists():
            raise FileNotFoundError(f'Query file not found: {query_file}')

        self.query_file = query_file
        self._query_fn = query_fn

        query_type = _detect_query_type(query_file)
        service_name = f'{node.get_name()}/query_services/{name}'

        if query_type == 'select':
            self._service = node.create_service(
                srv_type=SelectQuery,
                srv_name=service_name,
                callback=self._select_callback,
            )
        elif query_type == 'ask':
            self._service = node.create_service(
                srv_type=AskQuery,
                srv_name=service_name,
                callback=self._ask_callback,
            )
        else:
            raise ValueError(f'Unknown query type "{query_type}", expected "select" or "ask"')

        self.logger.info(f'Query service "{service_name}" ready ({query_type})')

    def _read_query(self) -> str:
        with open(self.query_file, 'r') as f:
            return f.read()

    def _select_callback(
        self,
        request: SelectQuery.Request,
        response: SelectQuery.Response,
    ) -> SelectQuery.Response:
        try:
            result = self._query_fn(self._read_query())
            response.success = result != ''
            response.result = result
            if not response.success:
                response.error_message = 'Query returned an empty result'
        except Exception as e:
            self.logger.error(f'Select query failed: {e}')
            response.success = False
            response.error_message = str(e)
        return response

    def _ask_callback(
        self,
        request: AskQuery.Request,
        response: AskQuery.Response,
    ) -> AskQuery.Response:
        try:
            raw = self._query_fn(self._read_query())
            response.success = raw != ''
            if response.success:
                # ASK queries serialise to {"boolean": true/false} in SPARQL JSON
                response.result = json.loads(raw).get('boolean', False)
            else:
                response.error_message = 'Query returned an empty result'
        except Exception as e:
            self.logger.error(f'Ask query failed: {e}')
            response.success = False
            response.error_message = str(e)
        return response
