from ros2cli.verb import VerbExtension

from triplestar_cli.api.bringup import discover_bringup_packages


class ListVerb(VerbExtension):
    """List the available triplestar query services."""

    def main(self, *, args):

        packages = discover_bringup_packages()

        for name in sorted(packages.keys()):
            print(name)
