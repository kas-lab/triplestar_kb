# Visualization

The `triplestar_viz` package contains a standalone Graphviz renderer, a ROS image publisher for a persistent Oxigraph store, and an RViz geometry publisher.

## Standalone graph rendering

The `rdfstar_viz` executable reads an RDF file or an Oxigraph store and writes an image:

```bash
# Render all triples from a Turtle file
ros2 run triplestar_viz rdfstar_viz data.ttl -o graph.svg

# Render the result of a SPARQL CONSTRUCT query
ros2 run triplestar_viz rdfstar_viz data.ttl \
  --query rooms.sparql -o rooms.png

# Open an existing store read-only
ros2 run triplestar_viz rdfstar_viz \
  --store /var/lib/triplestar/store -o graph.pdf
```

The output extension selects PNG, SVG, PDF, or JPEG. Available Graphviz engines are `dot`, `neato`, `fdp`, `sfdp`, `circo`, and `twopi`; `sfdp` is the default. Graphviz executables must be installed and available on `PATH`.

The renderer distinguishes named nodes, literals, blank nodes, and RDF-star triple terms. It shortens known namespaces and can include a prefix legend.

### Python use

The same implementation is available without a ROS node:

```python
from triplestar_viz.core import RDFLoader, RDFStarVisualizer

store = RDFLoader.from_file("data.ttl")
visualizer = RDFStarVisualizer(format="SVG", engine="dot")
svg = visualizer.generate_visualization(store=store)
```

## ROS graph image publisher

Run `kb_visualizer_node` with the same persistent store path configured by the bringup package:

```bash
ros2 run triplestar_viz kb_visualizer_node \
  --ros-args -p store_path:=/var/lib/triplestar/store
```

The node opens that store read-only. It starts publishing after a `SetVizQuery` request configures an update interval:

```bash
ros2 service call /set_viz_query triplestar_msgs/srv/SetVizQuery \
  "{query: '', update_rate: 1.0}"
```

An empty query renders the whole store. A non-empty query should be a SPARQL `CONSTRUCT`. With the default node name and namespace, images are published as `sensor_msgs/Image` on `/kb_visualizer/graph_visualization/image`.

## RViz geometry markers

The generated bringup launch file can enable `kb_geometry_visualizer`:

```bash
ros2 triplestar bringup launch my_bringup enable-geometry-viz:=true
```

The node calls `/triplestar/sparql` once per second for resources connected through `geo:hasGeometry/geo:asWKT`. It publishes a `visualization_msgs/MarkerArray` on `/kb_markers` in the `map` frame.

| WKT geometry | RViz marker |
| --- | --- |
| `Polygon` | `LINE_STRIP` |
| `LineString` | `LINE_STRIP` |
| `Point` | `SPHERE` |

Optional `rdfs:label` values become text markers. In RViz, add a `MarkerArray` display for `/kb_markers` and ensure the fixed frame can resolve `map`.

!!! note
    The geometry node depends on the general SPARQL service, so keep at least one file-backed query configured and the core lifecycle node active.
