# TriplestarKB

TriplestarKB is a ROS2-enabled knowledge base, backed by the [oxigraph](https://github.com/oxigraph/oxigraph) SPARQL graph database.

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
To generate a new bringup package, run the following from the `src` folder of your workspace.
```bash
cd src
python3 -m cookiecutter triplestar_kb/bringup_template
```

This will result in a new bringup package with your chosen name in the `src` folder.

### Build the package

```bash
colcon build --symlink-install --merge-install --cmake-args -DCMAKE_EXPORT_COMPILE_COMMANDS=ON --packages-select {your_custom_bringup_name}
```

### Running

After sourcing the workspace, run triplestar with your custom config using:
```bash
ros2 launch {your_custom_bringup_name} {your_custom_bringup_name}_triplestar.launch.xml
```


## ROS to RDF conversions

ROS(2) defines its own set of interfaces (messages and services) for representing data like timestamps, points, integers, floats, polygons etc.
To facilitate the integration of these datatypes into the kb, which is based on RDF, these types need to be converted to suitable RDF types.

| ROS msg type                                                                           | Python / Shapely type      | RDF literal type |
| -------------------------------------------------------------------------------------- | -------------------------- | ---------------- |
| `geometry_msgs/Point`, `Point32`, `PointStamped`                                       | `shapely.geometry.Point`   | `geo:wktLiteral` |
| `geometry_msgs/Pose`                                                                   | `shapely.geometry.Point`   | `geo:wktLiteral` |
| `geometry_msgs/Vector3`, `Vector3Stamped`                                              | `shapely.geometry.Point`   | `geo:wktLiteral` |
| `geometry_msgs/Polygon`, `PolygonStamped`, `PolygonInstance`, `PolygonInstanceStamped` | `shapely.geometry.Polygon` | `geo:wktLiteral` |
| `builtin_interfaces/Time`                                                              | `datetime.datetime`        | `xsd:dateTime`   |
| `std_msgs/Float32`, `Float64`                                                          | `float`                    | `xsd:float`      |
| `std_msgs/Int8`, `Int16`, `Int32`, `Int64`                                             | `int`                      | `xsd:integer`    |
| `std_msgs/UInt8`, `UInt16`, `UInt32`, `UInt64`                                         | `int`                      | `xsd:integer`    |
| `std_msgs/Char`, `Byte`                                                                | `int`                      | `xsd:integer`    |
| `std_msgs/Bool`                                                                        | `bool`                     | `xsd:boolean`    |
| `std_msgs/String`                                                                      | `str`                      | `xsd:string`     |

## Query-time subscribers

Some data, such as room geometries or class hirarchies, will be quite static in your KB, while other data, such as the robots own location or battery level, will change frequently.
For this requently-chaning information it is possible to add _query_time_subscribers_ to the kb node, which keep track of the messages published on a certain topic and expose them to oxigraph's underlying SPARQL evaluator to be run at query time.
As an example:

```sparql
PREFIX ex: <http://example.org/>
SELECT ?bl WHERE {
  BIND(ex:robotBatteryLevel() AS ?bl) .
  FILTER(?bl > 0.2)
}
```

This query contains the special function `robotBatteryLevel`, which accesses the latest message on a certain topic at query time.
These query time subscribers can be added by modifying the config file.

## Query-time TF subscribers

ROS(2) makes use of the `tf2` library to publish transforms between coordinate frames

- _Pellissier Tanon, T._ (n.d.). **Oxigraph**. [![DOI:10.5281/zenodo.7408022](https://zenodo.org/badge/DOI/10.5281/zenodo.7408022.svg)](https://doi.org/10.5281/zenodo.7408022)
