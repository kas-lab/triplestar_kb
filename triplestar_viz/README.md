# TriplestarKB visualization

This package provides standalone RDF and RDF-star rendering with Graphviz, a ROS image publisher for an Oxigraph store, and an RViz geometry marker publisher.

After building and sourcing the package, render a Turtle file with:

```bash
ros2 run triplestar_viz rdfstar_viz data.ttl -o graph.svg
```

See the [visualization guide](https://kas-lab.github.io/triplestar_kb/visualization/) for supported inputs, layout engines, ROS node behavior, service names, and RViz setup.
