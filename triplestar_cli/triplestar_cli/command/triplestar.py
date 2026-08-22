from ros2cli.command import CommandExtension
from ros2cli.command import add_subparsers_on_demand


class TriplestarCommand(CommandExtension):
    """Various triplestar knowledge base related sub-commands."""

    def add_arguments(self, parser, cli_name, *, argv=None):
        self._subparser = parser
        add_subparsers_on_demand(
            parser,
            cli_name,
            '_verb',
            'triplestar.verb',
            required=False,
            argv=argv,
        )

    def main(self, *, parser, args):
        if not hasattr(args, '_verb'):
            self._subparser.print_help()
            return 0

        extension = args._verb
        return extension.main(args=args)
