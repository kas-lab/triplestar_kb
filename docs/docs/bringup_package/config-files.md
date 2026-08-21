---
icon: lucide/settings
---

# Config files

The bringup package contains three YAML configuration files. Each is loaded by the `TriplestarKBNode` during the configure lifecycle transition.

---

## `kb_params.yaml`

Core knowledge base settings.

```yaml
# Where to persist the Oxigraph store folder on disk.
# The store is created/opened at this path on startup.
store_path: "/tmp/triplestar_kb"

# Base IRI for resolving relative IRIs in SPARQL queries and updates.
# With base_iri: "http://triplestar.local":
#   :robotA  →  <http://triplestar.local/robotA>
# Custom functions are available as prefix fn:
#   fn:myFunction  →  <http://triplestar.local/functions/myFunction>
# Query-time functions use prefix qt:
#   qt:batteryLevel  →  <http://triplestar.local/query-time/batteryLevel>
base_iri: "http://triplestar.local"

# Whether to clear all triples from the store at startup.
# Set to false to persist data across restarts.
clear_on_startup: true

# Turtle (.ttl) files to load from the preload/ directory at startup.
preload_files:
  - example_data.ttl
  - geometry.ttl
```

| Field | Type | Required | Description |
|---|---|---|---|
| `store_path` | `str` (path) | Yes | Filesystem path for the Oxigraph store persistence. Use `""` for in-memory only. |
| `base_iri` | `str` (IRI) | Yes | Base IRI for resolving relative IRIs in SPARQL. Also provides `fn:` and `qt:` prefix namespaces. |
| `clear_on_startup` | `bool` | No (default `true`) | If `true`, wipes the store before loading preload files. |
| `preload_files` | `list[str]` | No (default `[]`) | List of `.ttl` filenames from the `preload/` directory to load at startup. |

---

## `query_services.yaml`

Binds SPARQL query files to ROS 2 services.

```yaml
query_services:
  count_triples:
    query_file: count_triples.sparql
    reasoning: false

  count_triples_reasoning:
    query_file: count_triples.sparql
    reasoning: true

  all_triples:
    query_file: get_all_triples.sparql
    reasoning: true

  # robot_pose:
  #     query_file: get_robot_pose.sparql
```

Each entry creates a ROS 2 service at `/triplestar/query/{name}`. The service type is auto-detected from the query file:

- `SELECT` → `triplestar_msgs/srv/SelectQuery` (returns JSON string)
- `ASK` → `triplestar_msgs/srv/AskQuery` (returns bool)

| Field | Type | Required | Description |
|---|---|---|---|
| `query_file` | `str` | Yes | Filename in the `queries/` directory. |
| `reasoning` | `bool` | No (default `false`) | If `true`, runs OWL 2 RL reasoning before executing the query, using the `reasonable` library. Inferred triples are placed in the `<base_iri>/reasoned-graph` named graph. |

### Calling a query service

```bash
# SELECT query → JSON result
ros2 service call /triplestar/query/count_triples triplestar_msgs/srv/SelectQuery {}

# With substitutions
ros2 service call /triplestar/query/count_triples triplestar_msgs/srv/SelectQuery \
  "{substitutions: [{variable: 's', rdf_term: '<http://example.org/robot1>'}]}"
```

---

## `subscribers.yaml`

Configures how the KB subscribes to ROS 2 topics.

```yaml
# ── Insertion subscribers ─────────────────────────────────────────────
# Run a Jinja2 SPARQL template on every received message and INSERT the
# result into the KB.
insertion_subscribers:
  my_detections:
    topic: "/detections"
    template: "ExampleInsertion.sparql.tmpl"

# ── Query-time topic subscribers ──────────────────────────────────────
# Keep the latest message on a topic and expose it as a SPARQL function
# callable at query time.
query_time_topic_subscribers:
  batteryLevel:
    topic: "/battery_level"

# ── Query-time TF subscribers ─────────────────────────────────────────
# Look up the latest transform between two frames and expose it as a
# SPARQL function callable at query time.
query_time_tf_subscribers:
  robotPose:
    from_frame: "base_link"
    to_frame: "map"
```

### Insertion subscribers

When a message arrives on the configured topic, the subscriber renders a [Jinja2](https://jinja.palletsprojects.com/) template with the message as the `msg` variable and executes the result as a SPARQL UPDATE.

The `rdf` filter is available in templates to convert ROS message fields to their RDF literal representation:

```jinja2
{% raw %}
PREFIX ex: <http://example.org/>

INSERT DATA {
  ex:sensor ex:timestamp {{ msg.header.stamp | rdf }} .
  ex:sensor ex:value {{ msg.value | rdf }} .
}
{% endraw %}
```

For a full list of supported ROS → RDF conversions, see the [ROS → RDF conversion reference](../concepts/ros-to-rdf.md).

### Query-time topic subscribers

Subscribes to a topic and caches the latest message. The value is exposed as a custom SPARQL function using the `qt:` prefix.

For example, with `batteryLevel` configured as above:

```sparql
PREFIX qt: <http://triplestar.local/query-time/>
SELECT ?robot ?battery WHERE {
  ?robot a <http://example.org/Robot> .
  BIND(qt:batteryLevel() AS ?battery)
  FILTER(?battery > 0.2)
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `topic` | `str` | Yes | Topic name to subscribe to. |
| `msg_field_name` | `str` | No | If set, returns only this field from the message (e.g., `data` for `std_msgs/Float32`). Otherwise the whole message is returned and converted via `to_rdf_literal()`. |

### Query-time TF subscribers

Exposes the latest transform between two coordinate frames as a SPARQL function.

```sparql
PREFIX qt: <http://triplestar.local/query-time/>
SELECT ?robot ?pose WHERE {
  ?robot a <http://example.org/Robot> .
  BIND(qt:robotPose() AS ?pose)
}
```

The function returns a `geo:wktLiteral` point representing the translation component of the transform.
