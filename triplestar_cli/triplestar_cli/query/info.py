from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from ros2cli.verb import VerbExtension

from triplestar_cli.api import QueryNameCompleter
from triplestar_cli.api import get_triplestar_query_info


class InfoVerb(VerbExtension):
    """Show information about a TriplestarKB query."""

    def add_arguments(self, parser, cli_name):
        arg = parser.add_argument(
            'name',
            help='Name of the query',
        )
        arg.completer = QueryNameCompleter()

    def main(self, *, args):
        info = get_triplestar_query_info(args.name)

        console = Console()

        table = Table(
            show_header=False,
            box=None,
            padding=(0, 1),
        )
        table.add_column(style='bold')
        table.add_column()

        table.add_row('Name', info.name)
        table.add_row('Type', info.type)

        console.print(
            Panel(
                table,
                title='Query',
                expand=False,
            )
        )

        console.print()

        console.print(
            Panel(
                Syntax(
                    info.sparql.strip(),
                    'text',
                    line_numbers=True,
                ),
                title='SPARQL',
            )
        )

        return 0
