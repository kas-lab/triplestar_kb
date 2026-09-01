# TriplestarKB bringup support

This package provides the shared TriplestarKB launch file, lifecycle launch helpers, and the template used by the CLI to generate application bringup packages.

Generate a package after building and sourcing the workspace:

```bash
ros2 triplestar bringup new --name my_bringup
```

Do not copy `triplestar_bringup` as an application configuration. The generated package registers itself with the ament index and includes the expected config, preload, query, template, function, and launch directories.

See the canonical documentation for:

- [Bringup package structure](https://kas-lab.github.io/triplestar_kb/bringup/)
- [Configuration fields](https://kas-lab.github.io/triplestar_kb/bringup/configuration/)
- [Custom SPARQL functions](https://kas-lab.github.io/triplestar_kb/bringup/custom-functions/)
