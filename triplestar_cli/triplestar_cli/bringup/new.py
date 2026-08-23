import os
from pathlib import Path

from ament_index_python import get_package_share_directory
from cookiecutter.main import cookiecutter
from ros2cli.verb import VerbExtension


def find_workspace_src() -> Path | None:
    """
    Return the ``src/`` directory of the active colcon workspace.

    colcon's setup scripts set ``COLCON_PREFIX_PATH`` to the workspace install
    directory; the source directory is its sibling. Falls back to
    ``AMENT_PREFIX_PATH`` for the ROS underlay case.
    """
    for var in ('COLCON_PREFIX_PATH', 'AMENT_PREFIX_PATH'):
        prefix = os.environ.get(var)
        if not prefix:
            continue
        install_dir = Path(prefix.split(os.pathsep)[0])
        src_dir = install_dir.parent / 'src'
        if src_dir.is_dir():
            return src_dir
    return None


class NewVerb(VerbExtension):
    """Generate a new TriplestarKB bringup package from the bundled template."""

    def add_arguments(self, parser, cli_name):
        parser.add_argument(
            '--name',
            '-n',
            help=(
                'Name of the bringup package to generate '
                '(e.g. my_bringup). Prompts interactively when omitted.'
            ),
        )
        parser.add_argument(
            '--output-dir',
            '-o',
            help=(
                'Directory to write the generated package into '
                '(default: the active workspace src/ directory)'
            ),
        )

    def main(self, *, args):
        share_dir = Path(get_package_share_directory('triplestar_bringup'))
        template_dir = share_dir / 'bringup_template'

        output_dir = args.output_dir
        if output_dir is None:
            output_dir = find_workspace_src()
        if output_dir is None:
            output_dir = '.'

        extra_context = {'bringup_name': args.name} if args.name else None
        generated = cookiecutter(
            str(template_dir),
            no_input=extra_context is not None,
            extra_context=extra_context,
            output_dir=str(output_dir),
        )

        print(f'Created bringup package at {generated}')
        return 0
