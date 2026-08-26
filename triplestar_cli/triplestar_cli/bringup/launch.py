import argparse

from ros2cli.verb import VerbExtension
from ros2launch.api import launch_a_launch_file

from triplestar_cli.api.bringup import BringupNameCompleter
from triplestar_cli.api.bringup import get_bringup_package_launch_file_path


class LaunchVerb(VerbExtension):
    """Launch a TriplestarKB bringup package."""

    def add_arguments(self, parser, cli_name):
        arg = parser.add_argument(
            'bringup_name',
            help='Name of the TriplestarKB bringup package to launch',
        )
        arg.completer = BringupNameCompleter()

        parser.add_argument(
            'launch_arguments',
            nargs=argparse.REMAINDER,
            help='Launch file arguments (e.g. enable-geometry-viz:=true)',
        )

    def main(self, *, args):
        launch_file_path = get_bringup_package_launch_file_path(args.bringup_name)
        return launch_a_launch_file(
            launch_file_path=launch_file_path,
            launch_file_arguments=args.launch_arguments,
        )
