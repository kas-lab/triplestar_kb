---
icon: lucide/settings
---

# Configuration file

Each bringup package contains one configuration file, `config/triplestar.yaml`. The
`TriplestarKBNode` loads it during the configure lifecycle transition.

```yaml
knowledge_base:
  store_path: "/tmp/triplestar_kb"
  base_iri: "http://triplestar.local"
  clear_on_startup: true
  preload_files:
    - example_data.ttl
    - geometry.ttl

insertion_subscribers:
  - topic: "/detections"
    template: "ExampleInsertion.sparql.tmpl"

insertion_services:
  - service: "/robot/set_enabled"
    template: "SetEnabledInsertion.sparql.tmpl"
    timeout_sec: 2.0

query_time_topic_subscribers:
  - topic: "/battery_state"
    sparql_fn_name: "batteryLevel"
    target_msg_field: "percentage"
  - topic: "/robot_status"
    sparql_fn_name: "statusStamp"
    target_msg_field: "header.stamp"

query_services:
  - query_file: "count_triples.sparql"
    service_name: "count_triples"
```

## Knowledge base

| Field | Type | Required | Description |
|---|---|---|---|
| `store_path` | `str` (path) | Yes | Filesystem path for Oxigraph persistence. Use `""` for an in-memory store. |
| `base_iri` | `str` (IRI) | Yes | Base IRI for relative IRIs and the `fn:` and `qt:` namespaces. |
| `clear_on_startup` | `bool` | No (default `true`) | Clear the store before loading preload files. |
| `preload_files` | `list[str]` | No (default `[]`) | `.ttl` filenames to load from `preload/`. |

For example, with `base_iri: "http://triplestar.local"`, `:robotA` resolves to
`<http://triplestar.local/robotA>`. Custom functions use the `fn:` prefix, while
query-time subscriber functions use `qt:`.

## Insertion subscribers

Each entry subscribes to `topic`, renders the named Jinja2 `template` with the
received message available as `msg`, and executes the result as a SPARQL update.
Templates are loaded from `templates/`.

The `rdf` filter converts supported ROS message values to RDF literals:

```jinja2
{% raw %}
PREFIX ex: <http://example.org/>

INSERT DATA {
  ex:sensor ex:timestamp {{ msg.header.stamp | rdf }} .
  ex:sensor ex:value {{ msg.value | rdf }} .
}
{% endraw %}
```

See the [ROS → RDF conversion reference](../concepts/ros-to-rdf.md) for supported
conversions.

## Insertion services

Each entry mirrors a target ROS 2 `service` below
`/triplestar/ingest/`. For example, `/robot/set_enabled` is exposed as
`/triplestar/ingest/robot/set_enabled`. Do not configure a service type. Triplestar
infers it from the live ROS graph and creates a target client and mirror with that
same type.

```yaml
insertion_services:
  - service: "/robot/set_enabled"
    template: "SetEnabledInsertion.sparql.tmpl"
    timeout_sec: 2.0
```

A call to the mirror is forwarded asynchronously to the target. After a successful
response, Triplestar renders the template with both `request` and `response`, applies
the resulting SPARQL update, and returns the target response unchanged:

```jinja2
{% raw %}
PREFIX ex: <http://example.org/>

INSERT DATA {
  ex:robot ex:enabled {{ response.success | rdf }} ;
           ex:lastCommand {{ request.data | rdf }} .
}
{% endraw %}
```

`timeout_sec` is optional and defaults to 2 seconds. It must be greater than zero.
If the target is unavailable when Triplestar activates, activation does not block or
fail. Discovery retries in the background, and the mirror appears after exactly one
loadable service type is advertised. Missing, unloadable, or ambiguous types are
logged and retried.

After a mirror has been created, a target that becomes unavailable or does not reply
within `timeout_sec` produces a default-initialized response of the service type. The
failure is logged and no insertion is applied. ROS 2 service responses have no
generic transport-error field, so callers that need to distinguish this fallback
should use a service type whose response contains an application-level success or
status field. Async forwarding, a reentrant callback group, and Triplestar's
multi-threaded executor keep these failures from blocking the node indefinitely.

## Query-time topic subscribers

Each entry caches the latest message from `topic` and exposes it as the
`qt:{sparql_fn_name}` SPARQL function. The optional `target_msg_field` selects a
message field; dotted paths such as `header.stamp` select nested fields. If omitted,
the whole message is converted.

```sparql
PREFIX qt: <http://triplestar.local/query-time/>
SELECT ?robot ?battery WHERE {
  ?robot a <http://example.org/Robot> .
  BIND(qt:batteryLevel() AS ?battery)
}
```

## Query-time TF positions

The built-in `qt:tfPosition(frame, referenceFrame)` function returns the origin of
`frame` expressed in `referenceFrame` as a `POINT Z` `geo:wktLiteral`. Frame names
are arguments, so they can be literals or values read from the knowledge base:

```sparql
PREFIX qt: <http://triplestar.local/query-time/>

SELECT ?frame ?position WHERE {
  ?robot <http://example.org/frameName> ?frame .
  BIND(qt:tfPosition(?frame, "map") AS ?position)
}
```

The lookup is non-blocking. If either frame is unknown or the latest transform is
about two seconds old, the expression has no value and its result remains unbound.

The `query_time_tf_subscribers` configuration is deprecated. Existing entries
continue to expose a zero-argument `qt:{sparql_fn_name}()` function, but new queries
should call `qt:tfPosition` directly. `tfPosition` is reserved and cannot be used as
a configured query-time subscriber function name.

## Query services

Each entry binds a `query_file` from `queries/` to the ROS 2 service named by
`service_name`. The service type is inferred from the query:

- `SELECT` → `triplestar_msgs/srv/SelectQuery` (JSON result)
- `ASK` → `triplestar_msgs/srv/AskQuery` (boolean result)

```bash
ros2 service call /triplestar/query/count_triples \
  triplestar_msgs/srv/SelectQuery {}
```
