import json

from lifecycle_msgs.msg import State
from lifecycle_msgs.srv import ChangeState
from lifecycle_msgs.srv import GetState
import rclpy
from ros2cli.node.strategy import NodeStrategy
from triplestar_core.service_contract import SPARQL_SERVICE_NAME

from triplestar_msgs.srv import SPARQLQuery

_TIMEOUT_SEC = 2.0
_TRIPLE_COUNT_QUERY = 'SELECT (COUNT(*) AS ?count) WHERE { ?s ?p ?o }'


def _service_name(node_name: str, suffix: str) -> str:
    return f'/{node_name.lstrip("/")}/{suffix}'


def get_lifecycle_state(node_name: str) -> State:
    """Return the current lifecycle state of the TriplestarKB node."""
    service_name = _service_name(node_name, 'get_state')

    with NodeStrategy({}) as node:
        client = node.create_client(GetState, service_name)

        if not client.wait_for_service(timeout_sec=_TIMEOUT_SEC):
            raise RuntimeError(
                f"Lifecycle node '{node_name}' is not available (no '{service_name}' service)"
            )

        future = client.call_async(GetState.Request())
        rclpy.spin_until_future_complete(node, future, timeout_sec=_TIMEOUT_SEC)

        response = future.result()
        if response is None:
            raise RuntimeError(f"Timed out waiting for '{service_name}'")

        return response.current_state


def change_lifecycle_state(node_name: str, transition_id: int) -> bool:
    """Request a lifecycle transition and return whether it succeeded."""
    service_name = _service_name(node_name, 'change_state')

    with NodeStrategy({}) as node:
        client = node.create_client(ChangeState, service_name)

        if not client.wait_for_service(timeout_sec=_TIMEOUT_SEC):
            raise RuntimeError(
                f"Lifecycle node '{node_name}' is not available (no '{service_name}' service)"
            )

        request = ChangeState.Request()
        request.transition.id = transition_id

        future = client.call_async(request)
        rclpy.spin_until_future_complete(node, future, timeout_sec=_TIMEOUT_SEC)

        response = future.result()
        if response is None:
            raise RuntimeError(f"Timed out waiting for '{service_name}'")

        return response.success


def get_triple_count() -> str:
    """Return the number of triples in the KB as a string."""
    with NodeStrategy({}) as node:
        client = node.create_client(SPARQLQuery, SPARQL_SERVICE_NAME)

        if not client.wait_for_service(timeout_sec=_TIMEOUT_SEC):
            raise RuntimeError(f"SPARQL service '{SPARQL_SERVICE_NAME}' is not available")

        request = SPARQLQuery.Request()
        request.query = _TRIPLE_COUNT_QUERY

        future = client.call_async(request)
        rclpy.spin_until_future_complete(node, future, timeout_sec=_TIMEOUT_SEC)

        response = future.result()
        if response is None:
            raise RuntimeError(f"Timed out waiting for '{SPARQL_SERVICE_NAME}'")

        if not response.success:
            raise RuntimeError(response.error_message or 'SPARQL query failed')

        data = json.loads(response.result)
        bindings = data['results']['bindings']
        if not bindings:
            return '0'

        value = next(iter(bindings[0].values()))
        return str(value.get('value', '0'))


def state_label(state: State) -> str:
    labels = {
        State.PRIMARY_STATE_UNCONFIGURED: 'unconfigured',
        State.PRIMARY_STATE_INACTIVE: 'inactive',
        State.PRIMARY_STATE_ACTIVE: 'active',
        State.PRIMARY_STATE_FINALIZED: 'finalized',
    }
    return state.label or labels.get(state.id, f'unknown ({state.id})')
