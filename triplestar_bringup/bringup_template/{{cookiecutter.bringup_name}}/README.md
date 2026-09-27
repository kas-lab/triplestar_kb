# {{cookiecutter.bringup_name}} - TriplestarKB Bringup Package

This generated package contains the scenario-specific configuration, queries,
functions, preload data, and insertion templates used to bring up TriplestarKB.
Its launch file delegates to `triplestar_bringup`, selects this package as the
configuration source, and can optionally enable the geometry visualizer.

Configure it in `config/triplestar.yaml`, which contains the `knowledge_base`,
`insertion_subscribers`, `insertion_services`, `query_time_topic_subscribers`,
`query_time_tf_subscribers`, and `query_services` sections. Query-time topic entries
may use `target_msg_field` to select a field, including a dotted nested path such as
`header.stamp`.

Launch it with:

```bash
ros2 launch {{cookiecutter.bringup_name}} bringup.launch.xml
```

See the [TriplestarKB configuration documentation](https://kas-lab.github.io/triplestar_kb/bringup_package/config-files/)
for configuration fields and package contents.
