# Architecture and lifecycle

TriplestarKB separates application configuration from the reusable ROS nodes. A bringup package is installed into the ament index and the core node resolves its share directory at runtime.

## Runtime components

| Component | Role |
| --- | --- |
| `TriplestarCoreNode` | Owns lifecycle transitions and loads the selected bringup package |
| `KnowledgeBase` | Wraps the Oxigraph store, namespaces, querying, updates, and reasoning |
| `SubscriptionManager` | Owns insertion, latest-topic, and latest-transform subscribers |
| `QueryServiceManager` | Owns the general SPARQL service and file-backed query services |
| Function registry | Collects decorated Python functions and registers them with Oxigraph |

## Lifecycle sequence

### Configure

The node:

1. Resolves the package named by its `bringup_package` parameter.
2. Loads `config/kb_params.yaml`.
3. Opens the Oxigraph store.
4. Clears the store when `clear_on_startup` is true, then loads configured Turtle files.
5. Parses subscriber and query-service configuration.
6. Imports Python modules from `functions/` and registers decorated functions.

Configuration failures return a lifecycle failure without starting live ROS subscriptions or query services.

### Activate

Activation discovers configured topic message types, creates subscriptions and TF support, registers query-time functions, and starts query services. Topic publishers that must be discovered should already be present in the ROS graph.

### Deactivate and clean up

Deactivation removes subscriptions, query-time functions, and query services while retaining the configured knowledge base. Cleanup releases the store-facing components and optimizes a persistent store before returning to an unconfigured state.

The shared bringup launch file requests configure and then activate. Lifecycle commands remain available for supervision:

```bash
ros2 triplestar status
ros2 triplestar stop
ros2 triplestar start
```

## Data paths

### Preload data

Configured `.ttl` files are loaded from the bringup package's `preload/` directory during configure. This path is intended for static or initial domain knowledge.

### Insertion subscribers

A ROS message is rendered through a Jinja2 template and the resulting SPARQL update is applied to the Oxigraph store. This path persists facts derived from a stream.

### Query-time subscribers

The latest topic value or TF translation is held by a ROS subscriber and exposed as a `qt:` SPARQL function. Calling the function reads current data without inserting it into the graph. Topic values older than the implementation's freshness window are returned as unavailable.

### Query services

A request executes against the store and can optionally run the configured reasoner first. Inferred triples are refreshed in a named graph derived from `base_iri`, and reasoning queries use the union of available graphs.

## Namespaces

For a `base_iri` of `http://triplestar.local`, the runtime provides:

| Prefix | Namespace |
| --- | --- |
| `:` | `http://triplestar.local` |
| `fn:` | `http://triplestar.local/functions/` |
| `qt:` | `http://triplestar.local/query-time/` |

Use a base IRI without a trailing slash to avoid doubled separators in the generated function namespaces.
