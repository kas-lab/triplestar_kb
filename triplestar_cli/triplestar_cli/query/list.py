from rich.console import Console
from rich.table import Table
from ros2cli.verb import VerbExtension
from triplestar_core.service_contract import QUERY_SERVICE_PREFIX

from triplestar_cli.api import get_triplestar_queries


class ListVerb(VerbExtension):
    """List the available triplestar query services."""

    def add_arguments(self, parser, cli_name):
        parser.add_argument(
            '--plain',
            action='store_true',
            help='Return plain strings instead of a formatted table.',
        )
        return parser

    def main(self, *, args):

        rows = [(query['name'], query['service_type']) for query in get_triplestar_queries()]

        if not rows:
            print(f'No query services found under {QUERY_SERVICE_PREFIX}')
            return 0

        if args.plain:
            for name, service_type in rows:
                print(f'{name}')
        else:
            table = Table(title='Triplestar Query Services')
            table.add_column('QUERY SERVICE')
            table.add_column('SERVICE TYPE')
            for name, service_type in rows:
                table.add_row(name, service_type)
            console = Console()
            console.print(table)
