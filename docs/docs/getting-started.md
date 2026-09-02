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

Build the core packages first:

```bash
colcon build --symlink-install --merge-install --packages-up-to triplestar_cli triplestar_viz
source install/setup.bash
```

Sourcing `install/setup.bash` makes the packages and the `ros2 triplestar`
command available in the current shell. Source it again in every new terminal.

## 4. Explore the CLI

```bash
ros2 triplestar --help
ros2 triplestar bringup list
```

The CLI manages bringup packages, launches the KB, reports its lifecycle state,
and calls configured query services. See the [CLI guide](cli.md) for the full
command map.

## 5. Run the KB with the reference bringup

The `triplestar_bringup` package is a reference configuration you can launch directly:

```bash
ros2 triplestar bringup launch triplestar_bringup
```

This starts a Lifecycle node that will:
1. Load `kb_params.yaml`, `subscribers.yaml`, and `query_services.yaml` from the bringup package's `config/` directory
2. Preload any `.ttl` files listed in `kb_params.yaml`
3. Start insertion subscribers, query-time subscribers, and query services as configured

You should see output like:

```
[INFO] [triplestar_core]: Configuring KB node...
[INFO] [triplestar_core]: Using store path: /tmp/triplestar_kb
[INFO] [triplestar_core]: KB node configured successfully
[INFO] [triplestar_core]: Activating KB node...
[INFO] [triplestar_core]: SPARQL query service ready at /triplestar/sparql
```

## 6. Inspect and query the KB

In another sourced terminal, inspect the node and discover its configured
queries:

```bash
ros2 triplestar status
ros2 triplestar query list
ros2 triplestar query info count_triples
ros2 triplestar query call count_triples
```

You can still call the generated ROS service directly when integrating or
debugging at the ROS layer:

```bash
ros2 service call /triplestar/query/count_triples triplestar_msgs/srv/SelectQuery {}
```

## 7. Create your own bringup package

TriplestarKB is configured per scenario using **bringup packages**. Generate one
from the bundled template:

```bash
ros2 triplestar bringup new --name my_robot_kb
```

This creates a new package with everything you need:

```
my_robot_kb/
├── config/
│   ├── kb_params.yaml           # Store path, base IRI, preload files
│   ├── query_services.yaml      # SPARQL queries as ROS services
│   └── subscribers.yaml         # Topic subscribers
├── preload/                     # .ttl files loaded at startup
├── queries/                     # SPARQL query files
├── templates/                   # Jinja2 insertion templates
├── functions/                   # Python SPARQL extension functions
├── launch/                      # ROS 2 launch file
├── CMakeLists.txt
└── package.xml
```

Build and run it:

```bash
colcon build --symlink-install --merge-install --packages-select my_robot_kb
source install/setup.bash
ros2 triplestar bringup launch my_robot_kb
```

## 8. Continue learning

- Learn how ROS messages become SPARQL updates in
  [Insertion templates](concepts/insertion-templates.md).
- Learn the graph-query basics in [Learn RDF and SPARQL](concepts/learning-sparql.md).
- Configure subscribers and query services with the
  [configuration reference](bringup_package/config-files.md).
- Render the graph or publish RViz markers with [Visualization](visualization.md).
