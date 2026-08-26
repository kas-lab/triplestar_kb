import os

from ament_index_python.packages import get_package_share_directory
from ament_index_python.resources import get_resource
from ament_index_python.resources import get_resources

RESOURCE_TYPE = 'triplestar_bringup'


def discover_bringup_packages() -> dict[str, str]:
    """Return ``pkg_name -> absolute path`` to its launch file."""
    result = {}
    for pkg_name in get_resources(RESOURCE_TYPE):
        content, _ = get_resource(RESOURCE_TYPE, pkg_name)
        rel_launch = content.strip()
        share_dir = get_package_share_directory(pkg_name)
        result[pkg_name] = os.path.join(share_dir, rel_launch)
    return result


def get_bringup_package_launch_file_path(pkg_name: str) -> str:
    """Return the absolute path to a bringup package's launch file."""
    launch_file = discover_bringup_packages().get(pkg_name)
    if not launch_file:
        raise ValueError(f'No bringup package found with name: {pkg_name}')
    return launch_file


class BringupNameCompleter:
    def __call__(self, prefix, parsed_args, **kwargs):
        return list(discover_bringup_packages().keys())
