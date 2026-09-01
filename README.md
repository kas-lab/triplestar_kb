# TriplestarKB

TriplestarKB is a ROS 2 knowledge base backed by [Oxigraph](https://github.com/oxigraph/oxigraph). It loads RDF data, integrates selected ROS topics and transforms, and exposes SPARQL queries through ROS 2 services.

**Documentation:** <https://kas-lab.github.io/triplestar_kb/>

## Packages

- `triplestar_core`: lifecycle-managed knowledge base runtime
- `triplestar_bringup`: shared launch support and bringup-package template
- `triplestar_cli`: `ros2 triplestar` commands
- `triplestar_msgs`: ROS interfaces
- `triplestar_viz`: Graphviz and RViz visualization

## Start here

Clone the repository into a ROS 2 workspace, install package dependencies with `rosdep`, and build:

```bash
cd ~/triplestar_ws/src
git clone https://github.com/kas-lab/triplestar_kb.git
cd ..
rosdep install --from-paths src/triplestar_kb --ignore-src -r -y
colcon build --symlink-install --packages-up-to triplestar_cli triplestar_viz
source install/setup.bash
```

TriplestarKB runs from an application-specific bringup package:

```bash
ros2 triplestar bringup new --name my_bringup
colcon build --symlink-install --packages-select my_bringup
source install/setup.bash
ros2 triplestar bringup launch my_bringup
```

See [Getting started](https://kas-lab.github.io/triplestar_kb/getting-started/) for configuration, verification, and troubleshooting.

## Contributing

Install [pre-commit](https://pre-commit.com/) and enable the repository hooks:

```bash
pip install pre-commit
pre-commit install
```

Documentation contributors should use the locked uv workflow in [`docs/README.md`](docs/README.md).

## Acknowledgement

TriplestarKB uses Oxigraph:

- _Pellissier Tanon, T._ (n.d.). **Oxigraph**. [![DOI:10.5281/zenodo.7408022](https://zenodo.org/badge/DOI/10.5281/zenodo.7408022.svg)](https://doi.org/10.5281/zenodo.7408022)
