from ros2cli.verb import VerbExtension
from tabulate import tabulate
from triplestar_core.service_contract import QUERY_SERVICE_PREFIX

from triplestar_cli.api import get_triplestar_queries


class ListVerb(VerbExtension):
    """List the available triplestar query services."""

    def main(self, *, args):

        rows = [(query['name'], query['service_type']) for query in get_triplestar_queries()]

        if not rows:
            print(f'No query services found under {QUERY_SERVICE_PREFIX}')
            return 0

        headers = ['QUERY SERVICE', 'SERVICE TYPE']
        print(tabulate(rows, headers=headers))
