import rclpy
from rclpy.node import Node
from ros2cli.node.strategy import NodeStrategy
from ros2service.api import get_service_names
from ros2service.api import get_service_names_and_types
from rosidl_runtime_py.utilities import get_service
from triplestar_core.service_contract import QUERY_SERVICE_PREFIX

from triplestar_msgs.msg import QuerySubstitution
from triplestar_msgs.srv import QueryInfo


def get_triplestar_query_names() -> list[str]:
    """Return the names of all available TripletarKB queries."""
    with NodeStrategy({}) as node:
        return [
            service_name.removeprefix(f'{QUERY_SERVICE_PREFIX}/')
            for service_name in get_service_names(node=node)
            if service_name.startswith(QUERY_SERVICE_PREFIX)
            and '/' not in service_name.removeprefix(f'{QUERY_SERVICE_PREFIX}/')
        ]


def get_triplestar_queries() -> list[dict[str, str]]:
    """Return all available TriplestarKB queries and their service types."""
    with NodeStrategy({}) as node:
        return [
            {
                'name': service_name.removeprefix(f'{QUERY_SERVICE_PREFIX}/'),
                'service_type': service_types[0],
            }
            for service_name, service_types in get_service_names_and_types(node=node)
            if service_name.startswith(QUERY_SERVICE_PREFIX)
            and '/' not in service_name.removeprefix(f'{QUERY_SERVICE_PREFIX}/')
        ]


def get_triplestar_query_info(name: str) -> QueryInfo.Response:
    """Return detailed information about a TriplestarKB query."""
    service_name = f'{QUERY_SERVICE_PREFIX}/{name}/info'

    with NodeStrategy({}) as node:
        client = node.create_client(QueryInfo, service_name)

        if not client.wait_for_service(timeout_sec=1.0):
            raise RuntimeError(f"Query info service '{service_name}' is not available")

        future = client.call_async(QueryInfo.Request())
        rclpy.spin_until_future_complete(node, future)

        response = future.result()
        if response is None:
            raise RuntimeError(f"Call to query info service '{service_name}' failed")

        return response


def call_triplestar_query(
    name: str,
    substitutions: list[str] | None = None,
    query: str | None = None,
    reasoning: bool | None = None,
    timeout: float = 10.0,
):
    """Call a TriplestarKB query service and return its response."""
    service_name = f'{QUERY_SERVICE_PREFIX}/{name}'

    with NodeStrategy({}) as node:
        try:
            service_class = get_service_class(node, service_name)
        except RuntimeError as e:
            raise RuntimeError(
                f"Service '{service_name}' not found. "
                'Run `ros2 triplestar query list` to see available services.'
            ) from e

        request = service_class.Request()

        if query is not None:
            if not hasattr(request, 'query'):
                raise RuntimeError(f"Query service '{name}' does not accept a SPARQL query")

            request.query = query

        if reasoning is not None:
            if not hasattr(request, 'reasoning'):
                raise RuntimeError(f"Query service '{name}' does not support reasoning")

            request.reasoning = reasoning

        if substitutions is not None:
            if not hasattr(request, 'substitutions'):
                raise RuntimeError(f"Query service '{name}' does not accept substitutions")

            for substitution in substitutions:
                variable, separator, rdf_term = substitution.partition('=')

                if not separator:
                    raise RuntimeError(f"Invalid substitution '{substitution}', expected VAR=TERM")

                request.substitutions.append(
                    QuerySubstitution(
                        variable=variable,
                        rdf_term=rdf_term,
                    )
                )

        client = node.create_client(service_class, service_name)

        if not client.wait_for_service(timeout_sec=timeout):
            raise RuntimeError(f"Service '{service_name}' is not available")

        future = client.call_async(request)

        rclpy.spin_until_future_complete(
            node,
            future,
            timeout_sec=timeout,
        )

        response = future.result()

        if response is None:
            raise RuntimeError(f"Call to '{service_name}' timed out")

        return response


def get_service_class(node: Node, service_name: str):
    """Return the ROS service class for a service name."""
    for name, service_types in get_service_names_and_types(node=node):
        if name != service_name:
            continue

        if not service_types:
            break

        return get_service(service_types[0])

    raise RuntimeError(f"Service '{service_name}' not found")


class QueryNameCompleter:
    def __call__(self, prefix, parsed_args, **kwargs):
        return get_triplestar_query_names()
