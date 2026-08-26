from lifecycle_msgs.msg import State
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from ros2cli.verb import VerbExtension

from triplestar_cli.api import DEFAULT_NODE_NAME
from triplestar_cli.api import get_triple_count
from triplestar_cli.api.lifecycle import get_lifecycle_state
from triplestar_cli.api.lifecycle import state_label

console = Console()


class StatusVerb(VerbExtension):
    """Show TriplestarKB lifecycle state and triple count."""

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
            state = get_lifecycle_state(node_name)
            active = state.id == State.PRIMARY_STATE_ACTIVE

            if active:
                try:
                    triples = get_triple_count()
                except RuntimeError as e:
                    triples = f'unknown ({e})'
            else:
                triples = 'unavailable (node inactive)'
        except RuntimeError as e:
            console.print(f'[bold red]Error:[/bold red] {e}')
            return 1

        table = Table(
            show_header=False,
            box=None,
            padding=(0, 1),
        )
        table.add_column(style='bold')
        table.add_column()

        table.add_row('Node', node_name)
        table.add_row('Running', 'yes' if active else 'no')
        table.add_row('State', state_label(state))
        table.add_row('Triples', triples)

        console.print(
            Panel(
                table,
                title='TriplestarKB',
                expand=False,
            )
        )

        return 0
