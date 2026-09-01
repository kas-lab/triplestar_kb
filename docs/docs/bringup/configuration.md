# Configuration

The core lifecycle node reads up to three YAML files from the selected bringup package. Pydantic models in `triplestar_core/config.py` define the accepted fields.

## `kb_params.yaml`

This file is required.

```yaml
store_path: "/var/lib/triplestar/store"
base_iri: "http://example.org/robot"
clear_on_startup: false
preload_files:
  - map.ttl
  - ontology.ttl
```

| Field | Type | Required | Behavior |
| --- | --- | --- | --- |
| `store_path` | Path | Yes | Directory opened by the Oxigraph store. The process needs permission to create or modify it. |
| `base_iri` | String | Yes | Resolves relative IRIs and forms the `fn:` and `qt:` namespaces. |
| `clear_on_startup` | Boolean | No, defaults to `true` | Clears the store during configure before preload data is loaded. |
| `preload_files` | List of strings | No, defaults to `[]` | `.ttl` filenames loaded from the installed `preload/` directory. |

A persistent deployment will normally set `clear_on_startup: false`. When it is `true`, the store is cleared during every configure attempt before preload files are checked.

## `query_services.yaml`

Each entry maps a service name to one query file under `queries/`:

```yaml
query_services:
  count_triples:
    query_file: count_triples.sparql
  robot_exists:
    query_file: robot_exists.sparql
```

| Field | Type | Required | Behavior |
| --- | --- | --- | --- |
| `query_file` | String | Yes | File read for each request. Its first `SELECT` or `ASK` keyword selects the ROS service type. |

Activation creates `/triplestar/query/<name>` and `/triplestar/query/<name>/info` for each valid entry. Reasoning is a request field, not a YAML setting; pass `--reasoning` to `ros2 triplestar query call` when needed.

The general `/triplestar/sparql` service is owned by the same manager. In the current implementation, it is started when `query_services.yaml` contains at least one configured query.

See [Querying](../querying.md) for calls, substitutions, and result formats.

## `subscribers.yaml`

This file is optional. It contains three independently optional mappings:

```yaml
insertion_subscribers:
  detections:
    topic: "/detections"
    template: "insert_detection.sparql.tmpl"

query_time_topic_subscribers:
  rosTime:
    topic: "/clock"
    msg_field_name: "clock"

query_time_tf_subscribers:
  robotPose:
    from_frame: "base_link"
    to_frame: "map"
```

An omitted mapping and an empty mapping are equivalent.

### Insertion subscribers

| Field | Type | Required | Behavior |
| --- | --- | --- | --- |
| `topic` | String | Yes | Topic whose message type is discovered during activation. |
| `template` | String | Yes | Jinja2 template loaded from `templates/`. |

The template receives the ROS message as `msg`. Jinja2 strict undefined handling turns missing fields into a logged insertion error instead of silently producing incomplete SPARQL.

Use the `rdf` filter to serialize supported values safely:

```jinja2
PREFIX ex: <http://example.org/>

INSERT DATA {
  ex:sensor ex:observedAt {{ msg.header.stamp | rdf }} .
  ex:sensor ex:value {{ msg.value | rdf }} .
}
```

The rendered text is sent to the Oxigraph update API. Test templates with representative messages and make sure every inserted term is valid SPARQL. See [ROS to RDF conversions](../concepts/message-conversions.md) for supported values.

### Query-time topic subscribers

| Field | Type | Required | Behavior |
| --- | --- | --- | --- |
| `topic` | String | Yes | Topic whose latest message is cached. |
| `msg_field_name` | String | No | Returns only this message field instead of converting the whole message. |

The mapping key becomes the function name. The `rosTime` example can be called as `qt:rosTime()`. The topic and its message type must be discoverable during activation. Values outside the current freshness window are unavailable rather than returned as stale data.

### Query-time TF subscribers

| Field | Type | Required | Behavior |
| --- | --- | --- | --- |
| `from_frame` | String | Yes | Source frame passed to the TF lookup. |
| `to_frame` | String | Yes | Target frame passed to the TF lookup. |

The mapping key again becomes the `qt:` function name. The function returns only the transform's translation, converted to a `geo:wktLiteral` point. Rotation is not included.

For the example above:

```sparql
PREFIX qt: <http://example.org/robot/query-time/>

SELECT ?pose WHERE {
  BIND(qt:robotPose() AS ?pose)
}
```
