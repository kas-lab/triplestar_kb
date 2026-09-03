# TriplestarKB

TriplestarKB is a ROS 2 enabled knowledge base backed by [Oxigraph](https://github.com/oxigraph/oxigraph), a high-performance SPARQL graph database.

 📖 **Full documentation**: [https://kas-lab.github.io/triplestar_kb](https://kas-lab.github.io/triplestar_kb) 

## Quick Start

### Repo and dependencies

First, clone the current repo into the `src` folder of your ROS2 workspace.
```bash
cd src
git clone https://github.com/kas-lab/triplestar_kb.git 
cd ..
```

Secondly, install the needed dependencies via rosdep:
```bash
rosdep install -i --from-path src/triplestar_kb -r -y
```

### Generate your own bringup package

*TriplestarKB* is configured on a per-scenario basis using _bringup packages_. 
To generate a new bringup package, run the following from your sourced workspace:
```bash
ros2 triplestar bringup new
```
You will be prompted for the package name. To skip the prompt, pass it with
`--name`:
```bash
ros2 triplestar bringup new --name my_bringup
```

The generated package is placed in the `src/` folder of the active colcon
workspace automatically. To write somewhere other than the workspace `src/`,
pass `--output-dir`:
```bash
ros2 triplestar bringup new --name my_bringup --output-dir /path/to/dir
```

### Build the package

```bash
colcon build --symlink-install --merge-install --cmake-args -DCMAKE_EXPORT_COMPILE_COMMANDS=ON --packages-select {your_custom_bringup_name}
```

### Running

After sourcing the workspace, run triplestar with your custom config using:
```bash
ros2 triplestar bringup launch {your_custom_brigup_name}
```

> :info: As seen in the above snippets, the `triplestar` cli provides convenient commands for common operations.


## Contributing

Contributions are welcome. Please install [pre-commit](https://pre-commit.com/)
and enable the hooks once:

    pip install pre-commit
    pre-commit install

This runs ruff (lint + format) automatically on every commit.

- _Pellissier Tanon, T._ (n.d.). **Oxigraph**. [![DOI:10.5281/zenodo.7408022](https://zenodo.org/badge/DOI/10.5281/zenodo.7408022.svg)](https://doi.org/10.5281/zenodo.7408022)
