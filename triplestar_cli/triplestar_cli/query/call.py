from rich.console import Console
from rich.json import JSON
from ros2cli.verb import VerbExtension

from triplestar_cli.api import QueryNameCompleter
from triplestar_cli.api import call_triplestar_query

console = Console()


class CallVerb(VerbExtension):
    """Call a TripleStar query service and print the result as JSON."""

    def add_arguments(self, parser, cli_name):
        arg = parser.add_argument(
            'query_name',
            help='Name of the TripleStar query to call',
        )
        arg.completer = QueryNameCompleter()

        parser.add_argument(
            '--query',
            '-q',
            help='SPARQL query text',
        )

        parser.add_argument(
            '--timeout',
            type=float,
            default=10.0,
            help='Service call timeout in seconds (default: 10.0)',
        )

        parser.add_argument(
            'substitutions',
            nargs='*',
            help='Query substitutions in VAR=TERM format',
        )

    def main(self, *, args):
        response = call_triplestar_query(
            name=args.query_name,
            query=args.query,
            substitutions=args.substitutions,
            timeout=args.timeout,
        )

        if response.success:
            console.print(JSON(response.result))
        else:
            console.print(f'[bold red]Error:[/bold red] {response.error_message}')

        return 0
