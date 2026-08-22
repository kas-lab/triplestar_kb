from ros2cli.node.strategy import NodeStrategy
from ros2cli.verb import VerbExtension
from rosidl_runtime_py.utilities import get_service
from tabulate import tabulate
from triplestar_core.service_contract import QUERY_SERVICE_PREFIX


class ListVerb(VerbExtension):
    """List the available triplestar query services."""

    def main(self, *, args):
        with NodeStrategy(args) as node:
            service_names_and_types = node.get_service_names_and_types()

        rows = [
            (
                name.removeprefix(QUERY_SERVICE_PREFIX),
                get_service(types[0]).__name__,
            )
            for name, types in service_names_and_types
            if name.startswith(QUERY_SERVICE_PREFIX)
        ]

        if not rows:
            print(f'No query services found under {QUERY_SERVICE_PREFIX}')
            return 0

        headers = ['QUERY SERVICE', 'KIND']
        print(tabulate(rows, headers=headers))
