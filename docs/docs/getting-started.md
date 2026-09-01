# Getting started

This guide builds TriplestarKB in a standard colcon workspace, creates an application-specific bringup package, and runs a first query.

## Prerequisites

You need:

- A sourced ROS 2 installation
- `colcon`, `rosdep`, and Git
- A shell supported by your ROS 2 installation

The repository does not declare a single supported ROS distribution. Use a distribution that can resolve the dependencies in the package manifests.

## Build TriplestarKB

Create a workspace and clone the repository into its `src` directory:

```bash
source /opt/ros/$ROS_DISTRO/setup.bash
mkdir -p ~/triplestar_ws/src
cd ~/triplestar_ws/src
git clone https://github.com/kas-lab/triplestar_kb.git
cd ..
```

Install the dependencies declared by the ROS packages, then build the CLI and visualization package with their workspace dependencies:

```bash
rosdep install --from-paths src/triplestar_kb --ignore-src -r -y
colcon build --symlink-install --packages-up-to triplestar_cli triplestar_viz
source install/setup.bash
```

## Generate a bringup package

TriplestarKB needs a bringup package for application-specific configuration. Generate one in the active workspace:

```bash
ros2 triplestar bringup new --name my_bringup
```

The command normally writes `my_bringup` under the active workspace's `src/` directory. Use `--output-dir /path/to/dir` to choose another destination.

The generated files are starter examples. Before deploying them on a robot, review `config/subscribers.yaml` and remove or replace example topics and frame names that your ROS graph does not provide.

Build and source the generated package:

```bash
cd ~/triplestar_ws
colcon build --symlink-install --packages-select my_bringup
source install/setup.bash
ros2 triplestar bringup list
```

`my_bringup` should now appear in the list. If it does not, confirm that the package built successfully and that the latest `install/setup.bash` is sourced in the current shell.

## Launch and inspect the knowledge base

Launch the generated package:

```bash
ros2 triplestar bringup launch my_bringup
```

The shared launch file configures and activates the lifecycle node automatically. Keep it running, then open another sourced terminal:

```bash
cd ~/triplestar_ws
source install/setup.bash
ros2 triplestar status
ros2 triplestar query list
ros2 triplestar query call count_triples
```

The final command calls the generated `count_triples.sparql` query and prints its JSON result.

## Next steps

- Replace the starter data and settings using the [bringup package guide](bringup/index.md).
- Learn the two query interfaces in [Querying](querying.md).
- Configure ROS topic and TF integration in [Configuration](bringup/configuration.md#subscribersyaml).
- Enable graph or geometry rendering with [Visualization](visualization.md).
