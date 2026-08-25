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

## 3. Build

Build the core packages first:

```bash
colcon build --symlink-install --merge-install --packages-select triplestar_core triplestar_msgs triplestar_viz
source install/setup.bash
```

## 4. Run the KB with the reference bringup

The `triplestar_bringup` package is a reference configuration you can launch directly:

```bash
ros2 launch triplestar_bringup triplestar_kb.launch.py
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

## 5. Query the KB

With the node running, call the SPARQL query service:

```bash
ros2 service call /triplestar/sparql triplestar_msgs/srv/SPARQLQuery \
  "{query: \"SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10\"}"
```

Or use the query services defined in `query_services.yaml`:

```bash
ros2 service call /triplestar/query/count_triples triplestar_msgs/srv/SelectQuery {}
```

## 6. Create your own bringup package

TriplestarKB is configured on a per-scenario basis using **bringup packages**. Use the [cookiecutter](https://github.com/cookiecutter/cookiecutter) template to scaffold a new one:

```bash
pip install cookiecutter
cd ~/ros2_ws/src
cookiecutter triplestar_kb/bringup_template
# You'll be prompted for a package name, e.g., "my_robot_kb"
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
ros2 launch my_robot_kb my_robot_kb_triplestar.launch.xml
```

## 7. Visualize the graph

Enable the geometry visualizer to see WKT geometries from the KB as RViz markers:

```bash
ros2 launch my_robot_kb my_robot_kb_triplestar.launch.xml enable-geometry-viz:=true
```

For full graph visualization (RDF\* with Graphviz), see the [Visualization](visualization.md) page.
