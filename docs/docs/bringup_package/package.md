---
icon: lucide/package
---

# Bringup package structure

A bringup package is the per-scenario configuration layer for TriplestarKB. It contains everything needed to tailor the KB to your robot, environment, and use case.

## Directory layout

```
my_bringup/
├── config/
│   ├── kb_params.yaml           # Core KB settings
│   ├── query_services.yaml      # SPARQL query → ROS service bindings
│   └── subscribers.yaml         # Topic subscribers configuration
├── preload/                     # Turtle (.ttl) files loaded at startup
├── queries/                     # SPARQL query files for query services
├── templates/                   # Jinja2 templates for insertion subscribers
├── functions/                   # Python SPARQL extension functions
├── launch/
│   └── my_bringup_triplestar.launch.xml
├── CMakeLists.txt
└── package.xml
```

## How it works

The KB's launch file (`triplestar_kb.launch.py`) takes a `bringup-package` argument (default: `triplestar_bringup`). At configure time, the `TriplestarKBNode` lifecycle node:

1. Resolves the bringup package's share directory via `ament_index_python.get_package_share_directory()`
2. Loads `config/kb_params.yaml` → [`KBConfig`][kbconfig-ref]
3. Loads `config/subscribers.yaml` → [`SubscribersConfig`][subconfig-ref]
4. Loads `config/query_services.yaml` → [`QueryServicesConfig`][qsconfig-ref]
5. Preloads `.ttl` files from `preload/`
6. Starts insertion subscribers, query-time subscribers, and query services
7. Discovers and registers custom SPARQL functions from `functions/`

[kbconfig-ref]: https://github.com/kas-lab/triplestar_kb/blob/main/triplestar_core/triplestar_core/config.py
[subconfig-ref]: https://github.com/kas-lab/triplestar_kb/blob/main/triplestar_core/triplestar_core/config.py
[qsconfig-ref]: https://github.com/kas-lab/triplestar_kb/blob/main/triplestar_core/triplestar_core/config.py

## Launch file

The generated launch file wraps the shared `triplestar_kb.launch.py` and sets the `bringup-package` argument:

```xml
<launch>
  <arg name="enable-geometry-viz" default="false" />

  <include file="$(find-pkg-share triplestar_bringup)/launch/triplestar_kb.launch.py">
      <arg name="bringup-package" value="my_bringup" />
      <arg name="enable-geometry-visualizer" value="$(var enable-geometry-viz)" />
  </include>
</launch>
```

## Creating a bringup package

Use the [cookiecutter](https://github.com/cookiecutter/cookiecutter) template:

```bash
pip install cookiecutter
cd ~/ros2_ws/src
cookiecutter triplestar_kb/bringup_template
```

This scaffolds the full directory structure with example files you can customize.
