from lifecycle_msgs.msg import State
from lifecycle_msgs.msg import Transition
from lifecycle_msgs.srv import ChangeState
from lifecycle_msgs.srv import GetState
import rclpy
from rich.console import Console
from ros2cli.node.strategy import NodeStrategy
from ros2cli.verb import VerbExtension

console = Console()

DEFAULT_NODE_NAME = 'triplestar_core'


def _service_name(node_name: str, suffix: str) -> str:
    return f'/{node_name.lstrip("/")}/{suffix}'


def _get_state(node, node_name: str) -> State:
    service_name = _service_name(node_name, 'get_state')
    client = node.create_client(GetState, service_name)

    if not client.wait_for_service(timeout_sec=2.0):
        raise RuntimeError(
            f"Lifecycle node '{node_name}' is not available (no '{service_name}' service)"
        )

    future = client.call_async(GetState.Request())
    rclpy.spin_until_future_complete(node, future, timeout_sec=2.0)

    response = future.result()
    if response is None:
        raise RuntimeError(f"Timed out waiting for '{service_name}'")

    return response.current_state


def _change_state(node, node_name: str, transition_id: int) -> ChangeState.Response:
    service_name = _service_name(node_name, 'change_state')
    client = node.create_client(ChangeState, service_name)

    if not client.wait_for_service(timeout_sec=2.0):
        raise RuntimeError(
            f"Lifecycle node '{node_name}' is not available (no '{service_name}' service)"
        )

    request = ChangeState.Request()
    request.transition.id = transition_id

    future = client.call_async(request)
    rclpy.spin_until_future_complete(node, future, timeout_sec=2.0)

    response = future.result()
    if response is None:
        raise RuntimeError(f"Timed out waiting for '{service_name}'")

    return response


class StartVerb(VerbExtension):
    """Activate the TriplestarKB lifecycle node."""

    def add_arguments(self, parser, cli_name):
        parser.add_argument(
            '--node',
            '-n',
            default=DEFAULT_NODE_NAME,
            help=f'Lifecycle node name (default: {DEFAULT_NODE_NAME})',
        )

    def main(self, *, args):
        node_name = args.node

        try:
            with NodeStrategy({}) as node:
                current_state = _get_state(node, node_name)

                if current_state.id == State.PRIMARY_STATE_ACTIVE:
                    console.print(f"'{node_name}' is already active")
                    return 0

                response = _change_state(node, node_name, Transition.TRANSITION_ACTIVATE)
        except RuntimeError as e:
            console.print(f'[bold red]Error:[/bold red] {e}')
            return 1

        if response.success:
            console.print(f"Activated '{node_name}'")
            return 0

        console.print(f"[bold red]Failed to activate '{node_name}'[/bold red]")
        return 1


class StopVerb(VerbExtension):
    """Deactivate the TriplestarKB lifecycle node."""

    def add_arguments(self, parser, cli_name):
        parser.add_argument(
            '--node',
            '-n',
            default=DEFAULT_NODE_NAME,
            help=f'Lifecycle node name (default: {DEFAULT_NODE_NAME})',
        )

    def main(self, *, args):
        node_name = args.node

        try:
            with NodeStrategy({}) as node:
                current_state = _get_state(node, node_name)

                if current_state.id == State.PRIMARY_STATE_INACTIVE:
                    console.print(f"'{node_name}' is already inactive")
                    return 0

                response = _change_state(node, node_name, Transition.TRANSITION_DEACTIVATE)
        except RuntimeError as e:
            console.print(f'[bold red]Error:[/bold red] {e}')
            return 1

        if response.success:
            console.print(f"Deactivated '{node_name}'")
            return 0

        console.print(f"[bold red]Failed to deactivate '{node_name}'[/bold red]")
        return 1
