# Bringup packages

A bringup package is the source of scenario-specific TriplestarKB behavior. Generate one rather than copying `triplestar_bringup` itself:

```bash
ros2 triplestar bringup new --name my_bringup
```

## Installed layout

```text
my_bringup/
├── config/
│   ├── kb_params.yaml
│   ├── query_services.yaml
│   └── subscribers.yaml
├── functions/
├── launch/
│   └── bringup.launch.xml
├── preload/
├── queries/
├── templates/
├── CMakeLists.txt
├── package.xml
└── README.md
```

| Path | Purpose |
| --- | --- |
| `config/kb_params.yaml` | Store path, base IRI, clearing policy, and preload files |
| `config/query_services.yaml` | Names and files for `SELECT` or `ASK` ROS services |
| `config/subscribers.yaml` | Insertion, latest-topic, and latest-TF subscriptions |
| `preload/` | Turtle data loaded during lifecycle configure |
| `queries/` | SPARQL files used by named query services |
| `templates/` | Jinja2 templates rendered by insertion subscribers |
| `functions/` | Decorated Python extension functions imported during configure |
| `launch/bringup.launch.xml` | Wrapper that selects this bringup package |

The generated `CMakeLists.txt` installs these directories and registers the launch file in the `triplestar_bringup` ament resource index. After changing package files, rebuild and source the package before launching it from a non-symlink install.

## Customize the starter

1. Choose a durable `store_path` and decide whether startup should clear it.
2. Replace `preload/example_data.ttl` with application data and update `preload_files`.
3. Define stable `SELECT` or `ASK` queries and register them in `query_services.yaml`.
4. Remove unused example subscriptions, then add only topics and transforms provided by your ROS graph.
5. Add insertion templates or [custom functions](custom-functions.md) only where the data flow needs them.
6. Update `package.xml` metadata and dependencies for your application.

See [Configuration](configuration.md) for the accepted YAML fields.

## Preload data and RDF 1.2

Only configured files with a `.ttl` suffix are loaded. A non-empty preload list that contains no valid files causes configuration to fail, so filenames must match installed files exactly.

When preload data uses RDF 1.2 triple terms or annotations, prefer [explicit reifiers](https://www.w3.org/TR/rdf12-turtle/#reified-triples). Anonymous reifiers can receive new blank-node identities on each load and produce apparent duplicates when the store is not cleared.

## Launching

The CLI discovers built and sourced bringup packages through the ament index:

```bash
ros2 triplestar bringup list
ros2 triplestar bringup launch my_bringup
```

Additional launch arguments follow the package name. For example:

```bash
ros2 triplestar bringup launch my_bringup enable-geometry-viz:=true
```
