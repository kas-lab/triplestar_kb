"""
Entry point for generating a new TriplestarKB bringup package.

Delegates to cookiecutter's CLI, exactly like running:

    python3 -m cookiecutter <triplestar_core>/bringup_template [extra args...]

The generated package is placed in the `src/` directory of the active colcon
workspace by default. An explicit `--output-dir`/`-o` still wins.
"""

import os
from pathlib import Path
import sys

from ament_index_python import get_package_share_directory
from cookiecutter.cli import main as cookiecutter_main


def find_workspace_src() -> Path | None:
    """
    Return the `src/` directory of the active colcon workspace, if any.

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


def _has_output_dir(args: list[str]) -> bool:
    for arg in args:
        if arg in ('-o', '--output-dir'):
            return True
        if arg.startswith(('-o=', '--output-dir=')):
            return True
    return False


def main():
    share_dir = Path(get_package_share_directory('triplestar_core'))
    template_dir = share_dir / 'bringup_template'

    user_args = sys.argv[1:]

    # Default the output directory to the workspace's src/ folder.
    if not _has_output_dir(user_args):
        src_dir = find_workspace_src()
        if src_dir is not None:
            user_args = ['--output-dir', str(src_dir), *user_args]

    # Behave like `python3 -m cookiecutter <template> [args...]`, prepending
    # the installed template path as cookiecutter's positional argument.
    sys.argv = [sys.argv[0], str(template_dir), *user_args]
    cookiecutter_main(prog_name='ros2 run triplestar_core new_bringup')


if __name__ == '__main__':
    main()
