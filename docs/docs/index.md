# TriplestarKB

TriplestarKB is a ROS 2 knowledge base backed by [Oxigraph](https://github.com/oxigraph/oxigraph). It connects ROS messages and transforms to an RDF graph that can be loaded, updated, and queried with SPARQL.

## What it provides

- An Oxigraph store managed by a ROS 2 lifecycle node
- Turtle preload data for static domain knowledge
- Jinja2 insertion templates that turn ROS topic messages into SPARQL updates
- Query-time functions backed by the latest topic value or TF translation
- Named ROS 2 services for file-backed `SELECT` and `ASK` queries
- A general service for ad hoc `SELECT` queries
- Python extension functions exposed in SPARQL under the `fn:` prefix
- Optional reasoning through the `reasonable` library
- Graphviz graph rendering and RViz geometry markers

TriplestarKB deliberately separates reusable runtime code from scenario-specific knowledge. A **bringup package** owns the store settings, data, queries, templates, subscriptions, extension functions, and launch file for one application.

## Packages

| Package | Responsibility |
| --- | --- |
| `triplestar_core` | Lifecycle node, Oxigraph store, conversions, subscriptions, query services, and extension functions |
| `triplestar_bringup` | Shared launch support and the template used to generate application bringup packages |
| `triplestar_cli` | `ros2 triplestar` commands for bringup, lifecycle, and query operations |
| `triplestar_msgs` | Message and service definitions |
| `triplestar_viz` | Standalone Graphviz rendering and ROS visualization nodes |

## Where to begin

1. Follow [Getting started](getting-started.md) to build the workspace and generate a bringup package.
2. Read [Bringup packages](bringup/index.md) before replacing the starter configuration.
3. Use [Querying](querying.md) to inspect the running knowledge base.
4. Consult [Architecture and lifecycle](concepts/architecture.md) when integrating publishers or lifecycle supervision.

For source code and issue tracking, see the [GitHub repository](https://github.com/kas-lab/triplestar_kb).
