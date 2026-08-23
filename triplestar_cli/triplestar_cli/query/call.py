import rclpy
from rich.console import Console
from rich.json import JSON
from ros2cli.node.strategy import NodeStrategy
from ros2cli.verb import VerbExtension
from rosidl_runtime_py.utilities import get_service
from triplestar_core.service_contract import QUERY_SERVICE_PREFIX

from triplestar_msgs.msg import QuerySubstitution
from triplestar_msgs.srv import SPARQLQuery

console = Console()


class CallVerb(VerbExtension):
    """Call a triplestar query service and print the result as JSON."""

    def add_arguments(self, parser, cli_name):
        parser.add_argument(
            'service_name',
            help="Short name of the query service to call (e.g. 'count_triples')",
        )
        parser.add_argument(
            '--query',
            '-q',
            help='SPARQL query text (required for /triplestar/sparql)',
        )
        parser.add_argument(
            '--sub',
            '-s',
            action='append',
            default=[],
            dest='substitutions',
            metavar='VAR=TERM',
            help='Substitute a SPARQL variable with an RDF term (repeatable)',
        )
        parser.add_argument(
            '--timeout',
            type=float,
            default=5.0,
            help='Service call timeout in seconds (default: 5.0)',
        )

    def main(self, *, args):
        service_name = QUERY_SERVICE_PREFIX + args.service_name

        with NodeStrategy(args) as node:
            service_type = next(
                (
                    types[0]
                    for name, types in node.get_service_names_and_types()
                    if name == service_name and types
                ),
                None,
            )
            if service_type is None:
                raise RuntimeError(
                    f"Service '{service_name}' not found. "
                    'Run `ros2 triplestar query list` to see available services.'
                )

            service_class = get_service(service_type)
            request = self._build_request(service_class, args)

            client = node.create_client(service_class, service_name)
            if not client.wait_for_service(timeout_sec=args.timeout):
                raise RuntimeError(f"Service '{service_name}' is not available")

            future = client.call_async(request)
            rclpy.spin_until_future_complete(node, future, timeout_sec=args.timeout)

        response = future.result()
        if response is None:
            raise RuntimeError(f"Call to '{service_name}' timed out")

        if response.success:
            console.print(JSON(response.result))
        else:
            console.print(f'[bold red]Error:[/bold red] {response.error_message}')

    @staticmethod
    def _build_request(service_class, args):
        request = service_class.Request()

        if service_class is SPARQLQuery:
            if not args.query:
                raise RuntimeError(
                    "A SPARQL query is required for '/triplestar/sparql' (use --query)"
                )
            request.query = args.query
            return request

        for substitution in args.substitutions:
            variable, _, rdf_term = substitution.partition('=')
            if not _:
                raise RuntimeError(f"Invalid substitution '{substitution}', expected VAR=TERM")
            request.substitutions.append(QuerySubstitution(variable=variable, rdf_term=rdf_term))

        return request
