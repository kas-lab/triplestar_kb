# Querying

TriplestarKB exposes named, file-backed queries for stable application interfaces and a general SPARQL service for interactive `SELECT` queries.

## File-backed query services

Each entry in `config/query_services.yaml` points to a query in the bringup package's `queries/` directory. At activation, TriplestarKB detects the first `SELECT` or `ASK` keyword and creates a typed service:

| Query form | Service type | Result |
| --- | --- | --- |
| `SELECT` | `triplestar_msgs/srv/SelectQuery` | SPARQL Results JSON in `result` |
| `ASK` | `triplestar_msgs/srv/AskQuery` | Boolean in `result` |

List and call these services through the CLI:

```bash
ros2 triplestar query list
ros2 triplestar query info count_triples
ros2 triplestar query call count_triples
```

Use `--reasoning` (or `-r`) to request reasoning for a call:

```bash
ros2 triplestar query call all_triples --reasoning
```

### Substitutions

A query can expose variables for safe initial bindings. Pass each binding as `VARIABLE=RDF_TERM`; quote the shell argument so angle brackets and quotes reach the CLI unchanged:

```bash
ros2 triplestar query call robots \
  'robot=<http://example.org/Robot1>' \
  'minimum="5"^^xsd:integer'
```

Variable names do not include `?`. RDF terms use Turtle/N3 syntax, such as `<http://example.org/Robot1>`, `"label"`, or `"5"^^xsd:integer`.

## Ad hoc SPARQL service

When file-backed query services are configured, activation also creates `/triplestar/sparql`. Call it directly for an ad hoc `SELECT` query:

```bash
ros2 service call /triplestar/sparql triplestar_msgs/srv/SPARQLQuery \
  "{query: 'SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10', reasoning: false}"
```

`SELECT` results are serialized as SPARQL Results JSON in the response's `result` field.

!!! note
    The knowledge-base query path supports `SELECT` and `ASK`. It rejects `CONSTRUCT` and `DESCRIBE`, and it is not a SPARQL update endpoint. Insertion subscribers perform configured updates. The standalone graph visualizer can use `CONSTRUCT` queries for rendering.

## Service names and lifecycle

A configured query named `rooms` creates:

- `/triplestar/query/rooms`
- `/triplestar/query/rooms/info`

The general endpoint is `/triplestar/sparql`. These services exist only while the core lifecycle node is active. See [Architecture and lifecycle](concepts/architecture.md) for the activation sequence.
