---
icon: lucide/graduation-cap
---

# Getting started

## Prerequisites

- ROS 2 (Jazzy or later) installed and sourced
- A ROS 2 workspace (e.g., `~/ros2_ws`)

## 1. Clone the repository

```bash
cd ~/ros2_ws/src
git clone https://github.com/kas-lab/triplestar_kb.git
cd ..
```

## 2. Install dependencies

```bash
rosdep install -i --from-path src/triplestar_kb -r -y
```

## 3. Build and source the workspace

Build the packages required to create and launch a custom bringup package:

```bash
colcon build --symlink-install --merge-install --packages-up-to triplestar_cli
source install/setup.bash
```

Sourcing `install/setup.bash` makes the packages and the `ros2 triplestar`
command available in the current shell. Source it again in every new terminal.

## 4. Create a bringup package

TriplestarKB requires a custom bringup package for its scenario-specific
configuration. The `triplestar_bringup` package supplies the shared launch file
and template, so it is not launched directly.

```bash
ros2 triplestar bringup new --name my_robot_kb
```

## 5. Build and discover your package

```bash
colcon build --symlink-install --merge-install --packages-select my_robot_kb
source install/setup.bash
ros2 triplestar bringup list
```

The list should include `my_robot_kb`.

## 6. Launch your custom bringup

```bash
ros2 triplestar bringup launch my_robot_kb
```

This starts the KB with the configuration from `my_robot_kb`.

## 7. Continue learning

- Learn how ROS messages become SPARQL updates in
  [Insertion templates](concepts/insertion-templates.md).
- Learn the graph-query basics in [Learn RDF and SPARQL](concepts/learning-sparql.md).
- Configure subscribers and query services with the
  [configuration reference](bringup_package/config-files.md).
