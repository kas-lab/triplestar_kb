from collections.abc import Callable
from pathlib import Path
import re
from typing import Literal

from opentelemetry import trace
from rclpy.lifecycle import LifecycleNode
from rclpy.node import Node

from triplestar_core.service_contract import QUERY_SERVICE_PREFIX
from triplestar_msgs.srv import AskQuery
from triplestar_msgs.srv import QueryInfo
from triplestar_msgs.srv import SelectQuery

QueryType = Literal['SELECT', 'ASK']

_SERVICE_TYPES: dict[QueryType, type] = {
    'SELECT': SelectQuery,
    'ASK': AskQuery,
}

TRACER = trace.get_tracer('triplestar_bench')


def _detect_query_type(query_file: Path) -> QueryType:
    sparql = query_file.read_text()
    stripped = re.sub(r'#[^\n]*', '', sparql)
    match = re.search(r'\b(SELECT|ASK)\b', stripped, re.IGNORECASE)
    if match is None:
        raise ValueError(f'Could not detect query type in "{query_file}" — expected SELECT or ASK')
    return match.group(1).upper()  # type: ignore


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
        self.query_file = query_file
        self.query_fn = query_fn
        self.name = name

        self.query_type = _detect_query_type(query_file)

        srv_name = QUERY_SERVICE_PREFIX + name
        info_srv_name = QUERY_SERVICE_PREFIX + name + '/info'

        match self.query_type:
            case 'SELECT':
                node.create_service(SelectQuery, srv_name, self._handle_select)
            case 'ASK':
                node.create_service(AskQuery, srv_name, self._handle_ask)
        node.create_service(QueryInfo, info_srv_name, self._handle_query_info)

        self.logger.info(f'Query service "{srv_name}" ready ({self.query_type})')

    @TRACER.start_as_current_span('run_query')
    def _run_query(self, request: SelectQuery.Request | AskQuery.Request) -> str | bool | None:
        # set tracing atributes
        span = trace.get_current_span()
        span.set_attribute('query_name', self.name)

        substitutions: dict[str, str] = {b.variable: b.rdf_term for b in request.substitutions}
        return self.query_fn(self.query_file.read_text(), substitutions)

    def _handle_select(self, request: SelectQuery.Request, response: SelectQuery.Response):
        try:
            result = self._run_query(request)
            if not isinstance(result, str):
                raise TypeError(f'Expected str result for SELECT query, got {type(result)}')
            response.success = True
            response.result = result
        except (TypeError, RuntimeError) as e:
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
        except (TypeError, RuntimeError) as e:
            self.logger.error(f'Query failed: {e}')
            response.success = False
            response.error_message = str(e)
        return response

    def _handle_query_info(self, _: QueryInfo.Request, response: QueryInfo.Response):
        response.name = self.name
        response.type = self.query_type
        response.sparql = self.query_file.read_text()

        return response
