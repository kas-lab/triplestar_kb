from collections.abc import Callable
from pathlib import Path
import re
from typing import Literal

from rclpy.lifecycle import LifecycleNode
from rclpy.node import Node
from triplestar_msgs.srv import AskQuery
from triplestar_msgs.srv import SelectQuery

QueryType = Literal['select', 'ask']

_SERVICE_TYPES: dict[QueryType, type] = {
    'select': SelectQuery,
    'ask': AskQuery,
}


def _detect_query_type(query_file: Path) -> QueryType:
    sparql = query_file.read_text()
    stripped = re.sub(r'#[^\n]*', '', sparql)
    match = re.search(r'\b(SELECT|ASK)\b', stripped, re.IGNORECASE)
    if match is None:
        raise ValueError(f'Could not detect query type in "{query_file}" — expected SELECT or ASK')
    return match.group(1).lower()  # type: ignore


class FileQueryService:
    def __init__(
        self,
        node: Node | LifecycleNode,
        name: str,
        query_file: Path,
        query_fn: Callable[[str, dict[str, str]], str | bool | None],
    ):
        self.logger = node.get_logger().get_child(name)
        if not query_file.exists():
            raise FileNotFoundError(f'Query file not found: {query_file}')
        self._query_file = query_file
        self._query_fn = query_fn

        query_type = _detect_query_type(query_file)
        srv_name = f'/triplestar/query/{name}'

        match query_type:
            case 'select':
                node.create_service(SelectQuery, srv_name, self._handle_select)
            case 'ask':
                node.create_service(AskQuery, srv_name, self._handle_ask)

        self.logger.info(f'Query service "{srv_name}" ready ({query_type})')

    def _run_query(self, request: SelectQuery.Request | AskQuery.Request) -> str | bool | None:
        substitutions: dict[str, str] = {b.variable: b.rdf_term for b in request.substitutions}
        return self._query_fn(self._query_file.read_text(), substitutions)

    def _handle_select(self, request: SelectQuery.Request, response: SelectQuery.Response):
        try:
            result = self._run_query(request)
            if not isinstance(result, str):
                raise TypeError(f'Expected str result for SELECT query, got {type(result)}')
            response.success = True
            response.result = result
        except Exception as e:
            self.logger.error(f'Query failed: {e}')
            response.success = False
            response.error_message = str(e)
        return response

    def _handle_ask(self, request: AskQuery.Request, response: AskQuery.Response):
        try:
            result = self._run_query(request)
            if not isinstance(result, bool):
                raise TypeError(f'Expected bool result for ASK query, got {type(result)}')
            response.success = True
            response.result = result
        except Exception as e:
            self.logger.error(f'Query failed: {e}')
            response.success = False
            response.error_message = str(e)
        return response
