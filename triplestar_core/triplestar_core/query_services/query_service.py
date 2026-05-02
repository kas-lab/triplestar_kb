import json
import re
from pathlib import Path
from typing import Callable, Literal

from rclpy.lifecycle import LifecycleNode
from rclpy.node import Node
from triplestar_msgs.srv import AskQuery, SelectQuery

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


class FileQueryService:
    def __init__(
        self,
        node: Node | LifecycleNode,
        name: str,
        query_file: Path,
        query_fn: Callable[[str], str],
    ):
        self.logger = node.get_logger().get_child(name)

        if not query_file.exists():
            raise FileNotFoundError(f'Query file not found: {query_file}')

        self._query = query_file.read_text()
        self._query_fn = query_fn
        self.logger = node.get_logger().get_child(name)

        query_type = _detect_query_type(query_file)
        srv_name = f'{node.get_name()}/query_services/{name}'

        srv_map = {
            'select': (SelectQuery, self._handle_select),
            'ask': (AskQuery, self._handle_ask),
        }

        if query_type not in srv_map:
            raise ValueError(f'Unknown query type "{query_type}"')

        srv_type, callback = srv_map[query_type]
        node.create_service(
            srv_type,
            srv_name,
            callback,
        )

        self.logger.info(f'Query service "{srv_name}" ready ({query_type})')

    def _handle_select(self, request, response):
        return self._handle(response, lambda r: r)

    def _handle_ask(self, request, response):
        return self._handle(response, lambda r: json.loads(r).get('boolean', False))

    def _handle(self, response, transform_fn):
        try:
            raw = self._query_fn(self._query)
            if not raw:
                response.success = False
                response.error_message = 'Query returned an empty result'
                return response

            response.success = True
            response.result = transform_fn(raw)
        except Exception as e:
            self.logger.error(f'Query failed: {e}')
            response.success = False
            response.error_message = str(e)
        return response
