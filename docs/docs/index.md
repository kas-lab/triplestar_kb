---
icon: lucide/vector-square
---

# TriplestarKB

**TriplestarKB** is a ROS 2 enabled knowledge base backed by [Oxigraph](https://github.com/oxigraph/oxigraph), a high-performance SPARQL graph database. It provides a bridge between the ROS 2 message ecosystem and the RDF semantic web stack.

## Why a knowledge base?

In robotics, knowledge is more than sensor readings — it's understanding rooms, objects, task hierarchies, and their relationships. A knowledge base stores this as a **semantic graph** (RDF triples) you can query with SPARQL, reason over, and integrate with your ROS 2 nodes.

## Key features

- **SPARQL query engine** — full [SPARQL 1.2](https://www.w3.org/TR/sparql12-query/) support via Oxigraph, with SELECT, ASK, UPDATE, CONSTRUCT, and reasoning
- **Automatic ROS → RDF conversion** — ROS messages (`geometry_msgs/Point32`, `std_msgs/Float32`, …) are seamlessly converted to typed RDF literals (WKT geometry, XSD types)
- **Insertion subscribers** — subscribe to ROS topics and translate incoming messages into SPARQL INSERT queries via [Jinja2](https://jinja.palletsprojects.com/) templates
- **Query-time subscribers** — expose the latest value on a ROS topic as a SPARQL function (`qt:batteryLevel()`) callable directly from queries
- **TF integration** — look up the latest transform between frames as a SPARQL function (`qt:robotPose()`)
- **Custom SPARQL functions** — register Python functions as `fn:` extension functions callable from SPARQL
- **Query services** — expose SPARQL queries as typed ROS 2 services (SELECT → JSON, ASK → bool)
- **Visualization** — visualize RDF\* graphs with [Graphviz](https://graphviz.org/) and publish geometry to RViz via the `triplestar_viz` package
- **ROS 2 lifecycle** — the KB node follows the lifecycle pattern (configure → activate → deactivate), with persistent storage to disk
- **Reasoning** — OWL 2 RL reasoning via the [reasonable](https://github.com/gtfierro/reasonable) library

## Package overview

| Package | Role |
|---|---|
| `triplestar_core` | The core KB node, SPARQL engine, ROS ↔ RDF conversions, subscriber management, query services, and custom functions |
| `triplestar_bringup` | Reference bringup package with example configs, queries, templates, and preload data |
| `triplestar_msgs` | ROS 2 message and service definitions for interacting with the KB |
| `triplestar_viz` | RDF\*/WKT geometry visualization (Graphviz CLI, RViz markers) |

## Next steps

- **[Getting started](getting-started.md)** — set up your workspace and run the KB
- **[Command-line interface](cli.md)** — create, launch, inspect, and query a KB
- **[Insertion templates](concepts/insertion-templates.md)** — turn ROS messages into RDF
- **[Learn RDF and SPARQL](concepts/learning-sparql.md)** — tutorials and standards references
- **[Bringup package structure](bringup_package/package.md)** — understand how to configure the KB for your scenario
- **[Config files reference](bringup_package/config-files.md)** — all configuration options explained
- **[Custom SPARQL functions](bringup_package/functions.md)** — extend the KB with Python functions
- **[Visualization](visualization.md)** — visualize your knowledge graph
