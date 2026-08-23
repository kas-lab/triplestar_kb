from ros2cli.command import add_subparsers_on_demand
from ros2cli.verb import VerbExtension


class BringupVerb(VerbExtension):
    """Manage TriplestarKB bringup packages."""

    def add_arguments(self, parser, cli_name, *, argv=None):
        self._subparser = parser
        add_subparsers_on_demand(
            parser,
            cli_name,
            '_subcommand',
            'triplestar.bringup.verb',
            required=False,
            argv=argv,
        )

    def main(self, *, args):
        if not hasattr(args, '_subcommand'):
            self._subparser.print_help()
            return 0

        extension = args._subcommand
        return extension.main(args=args)
