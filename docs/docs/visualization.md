---
icon: lucide/eye
---

# Visualization

The `triplestar_viz` package provides two visualization tools.

## Graph visualization (RDF\* with Graphviz)

Visualize the knowledge graph as rendered diagrams using [Graphviz](https://graphviz.org/). Supports RDF\* quoted triples.

### CLI

```bash
# Visualize a Turtle file
ros2 run triplestar_viz rdfstar-viz data.ttl -o output.svg

# Apply a SPARQL CONSTRUCT query first
ros2 run triplestar_viz rdfstar-viz kb.ttl --query rooms.rq -o rooms.png

# Load from an Oxigraph store
ros2 run triplestar_viz rdfstar-viz --store /tmp/kb_store -o graph.pdf
```

### Layout engines

| Engine | Use case |
|---|---|
| `sfdp` (default) | Large undirected graphs |
| `dot` | Hierarchical / directed layouts |
| `neato` | Spring model |
| `fdp` | Force-directed |
| `circo` | Circular layouts |

```bash
ros2 run triplestar_viz rdfstar-viz data.ttl -o output.svg --engine neato
```

### Node styling

- **URIs**: Blue ellipses
- **Literals**: Green rounded boxes
- **Blank nodes**: Gray diamonds
- **Quoted triples**: Dashed gray lines

### Python API

```python
from triplestar_viz.core import RDFLoader, RDFStarVisualizer

store = RDFLoader.from_file("kb.ttl")
viz = RDFStarVisualizer(format="SVG", engine="sfdp")
svg_data = viz.generate_visualization(store=store)
```

### ROS 2 node

```bash
ros2 run triplestar_viz kb_visualizer_node \
  --ros-args -p store_path:=/tmp/kb_store
```

Publishes the visualization to `~/graph_visualization/image` and provides a `~/set_viz_query` service to change the SPARQL CONSTRUCT query at runtime.

---

## Geometry visualizer (RViz markers)

The geometry visualizer periodically queries the KB for WKT geometries and publishes them as [visualization_msgs/MarkerArray](https://docs.ros2.org/latest/api/visualization_msgs/msg/MarkerArray.html) to the `/kb_markers` topic.

### How it works

The visualizer runs a SPARQL query that looks for entities with `geo:hasGeometry/geo:asWKT` properties:

```sparql
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?entity ?wkt ?label
WHERE {
    ?entity geo:hasGeometry/geo:asWKT ?wkt .
    OPTIONAL { ?entity rdfs:label ?label . }
}
```

Supported geometry types:

| WKT type | RViz marker type |
|---|---|
| `Polygon` | `LINE_STRIP` |
| `LineString` | `LINE_STRIP` |
| `Point` | `SPHERE` |

Each entity gets a deterministic color derived from its URI hash, and labels are rendered as `TEXT_VIEW_FACING` markers.

### Enabling the visualizer

Pass `enable-geometry-viz:=true` to your bringup launch file:

```bash
ros2 launch my_robot_kb my_robot_kb_triplestar.launch.xml enable-geometry-viz:=true
```

Or set the argument directly when using `triplestar_bringup`:

```bash
ros2 launch triplestar_bringup triplestar_kb.launch.py enable-geometry-visualizer:=true
```

Then in RViz, add a `MarkerArray` display subscribed to `/kb_markers`.
