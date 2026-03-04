# Triplestar KB Visualization

RDF\* graph visualization using Graphviz.

## Features

- 🎨 Visualize RDF\* graphs with quoted triples
- 📦 CLI, Python library, and ROS2 node
- 🔍 SPARQL CONSTRUCT query support
- 🎯 Multiple layout engines (sfdp, dot, neato, etc.)
- 🏷️ Automatic prefix shortening and legends

## Installation

### ROS2 Package

```bash
colcon build --packages-select triplestar_kb_viz
source install/setup.bash
```

## Usage

### CLI

```bash
ros2 run triplestar_kb_viz rdfstar-viz data.ttl -o output.svg
ros2 run triplestar_kb_viz rdfstar-viz kb.ttl --query rooms.rq -o rooms.png
ros2 run triplestar_kb_viz rdfstar-viz --store /tmp/kb_store -o graph.pdf
```

### Python

```python
from triplestar_kb_viz.core import RDFLoader, RDFStarVisualizer

store = RDFLoader.from_file("kb.ttl")
viz = RDFStarVisualizer(format="SVG", engine="sfdp")
svg_data = viz.generate_visualization(store=store)
```

### ROS2 Node

```bash
ros2 run triplestar_kb_viz kb_visualizer_node \
  --ros-args -p store_path:=/tmp/kb_store
```

Publishes to `~/graph_visualization/image` and provides `~/set_viz_query` service.

## Visualization

Different node types are styled distinctly:

- **URIs**: Blue ellipses
- **Literals**: Green rounded boxes
- **Blank nodes**: Gray diamonds
- **Quoted triples**: Dashed gray lines

Layout engines: **sfdp** (default), **dot**, **neato**, **fdp**, **circo**, **twopi**
