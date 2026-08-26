from lifecycle_msgs.msg import State
from lifecycle_msgs.msg import Transition
from rich.console import Console
from ros2cli.verb import VerbExtension

from triplestar_cli.api import DEFAULT_NODE_NAME
from triplestar_cli.api import change_lifecycle_state
from triplestar_cli.api import get_lifecycle_state

console = Console()


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
            current_state = get_lifecycle_state(node_name)

            if current_state.id == State.PRIMARY_STATE_ACTIVE:
                console.print(f"'{node_name}' is already active")
                return 0

            success = change_lifecycle_state(node_name, Transition.TRANSITION_ACTIVATE)
        except RuntimeError as e:
            console.print(f'[bold red]Error:[/bold red] {e}')
            return 1

        if success:
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
            current_state = get_lifecycle_state(node_name)

            if current_state.id == State.PRIMARY_STATE_INACTIVE:
                console.print(f"'{node_name}' is already inactive")
                return 0

            success = change_lifecycle_state(node_name, Transition.TRANSITION_DEACTIVATE)
        except RuntimeError as e:
            console.print(f'[bold red]Error:[/bold red] {e}')
            return 1

        if success:
            console.print(f"Deactivated '{node_name}'")
            return 0

        console.print(f"[bold red]Failed to deactivate '{node_name}'[/bold red]")
        return 1
