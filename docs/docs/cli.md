---
icon: lucide/terminal
---

# Command-line interface

Triplestar extends the ROS 2 CLI. Every command therefore starts with
`ros2 triplestar`, and the workspace containing `triplestar_cli` must be
sourced first:

```bash
source /opt/ros/jazzy/setup.bash
source ~/ros2_ws/install/setup.bash
ros2 triplestar --help
```

## Typical workflow

```bash
# See which bringup packages are installed
ros2 triplestar bringup list

# Launch one (this command stays in the foreground)
ros2 triplestar bringup launch my_robot_kb

# In a second sourced terminal
ros2 triplestar status
ros2 triplestar query list
ros2 triplestar query call count_triples
```

## Command map

| Command | Purpose |
|---|---|
| `ros2 triplestar bringup new` | Generate a bringup package |
| `ros2 triplestar bringup list` | List installed bringup packages |
| `ros2 triplestar bringup launch NAME` | Launch a bringup package |
| `ros2 triplestar status` | Show lifecycle state and triple count |
| `ros2 triplestar start` | Activate an inactive KB node |
| `ros2 triplestar stop` | Deactivate an active KB node |
| `ros2 triplestar query list` | List configured query services |
| `ros2 triplestar query info NAME` | Show a query's type and SPARQL text |
| `ros2 triplestar query call NAME` | Execute a configured query |

Add `--help` after any command to see its current arguments.

## Create a bringup package

With an active colcon workspace, the default output directory is its `src/`
directory:

```bash
ros2 triplestar bringup new --name my_robot_kb
```

Choose an explicit location when the workspace cannot be detected:

```bash
ros2 triplestar bringup new \
  --name my_robot_kb \
  --output-dir ~/ros2_ws/src
```

Build and source the generated package before it can be discovered:

```bash
cd ~/ros2_ws
colcon build --symlink-install --packages-select my_robot_kb
source install/setup.bash
ros2 triplestar bringup list
```

## Launch a bringup package

Arguments after the package name are forwarded to its launch file:

```bash
ros2 triplestar bringup launch my_robot_kb enable-geometry-viz:=true
```

The launch command occupies the terminal until the launch is stopped. Use a
second sourced terminal for `status` and `query` commands.

## Work with query services

List query names for scripting with `--plain`:

```bash
ros2 triplestar query list --plain
```

Inspect and call a configured query:

```bash
ros2 triplestar query info objects_in_room
ros2 triplestar query call objects_in_room room='<http://example.org/kitchen>'
```

Substitutions use `VARIABLE=RDF_TERM` syntax. The value is an RDF term, so IRIs
need angle brackets and strings need RDF quotes. Use `--reasoning` to override
the call with reasoning enabled, or `--timeout SECONDS` for a slower query:

```bash
ros2 triplestar query call objects_in_room \
  --reasoning --timeout 30 \
  room='<http://example.org/kitchen>'
```

## Target a non-default node

Lifecycle commands default to `/triplestar_kb`. Override that name with
`--node`:

```bash
ros2 triplestar status --node /robot1/triplestar_kb
ros2 triplestar stop --node /robot1/triplestar_kb
ros2 triplestar start --node /robot1/triplestar_kb
```

`start` and `stop` activate and deactivate the lifecycle node; they do not
launch or terminate its process.

## Troubleshooting

`ros2: error: argument command: invalid choice: 'triplestar'`
: Build `triplestar_cli`, then source the workspace's `install/setup.bash`.

No bringup packages are listed
: Build the bringup package and source the same workspace. Discovery uses the
  ROS 2 ament index, not the source directory alone.

The status command cannot find the node
: Keep the bringup launch running, check `ros2 node list`, and pass `--node` if
  the KB is namespaced or renamed.

No query services are listed
: Check that the node configured successfully and that
  `config/query_services.yaml` contains entries.
