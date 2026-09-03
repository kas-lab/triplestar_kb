---
icon: lucide/file-input
---

# Insertion templates

An insertion subscriber turns each ROS 2 message into a SPARQL update:

```text
ROS topic -> typed ROS message -> Jinja2 template -> SPARQL UPDATE -> RDF graph
```

Use one when topic data should become persistent, queryable knowledge. If you
only need the latest topic value while a query runs, use a query-time subscriber
instead.

## Configure the subscriber

Add an entry to `config/subscribers.yaml` in your bringup package:

```yaml
insertion_subscribers:
  battery_readings:
    topic: /battery
    template: battery-reading.sparql.tmpl
```

At configuration time, Triplestar resolves the topic's message type and loads
the named file from the bringup package's `templates/` directory. Every incoming
message is available to that template as `msg`.

## Write the template

For a `sensor_msgs/msg/BatteryState` message, create
`templates/battery-reading.sparql.tmpl`:

```jinja2
{% raw %}
PREFIX ex: <https://example.org/robot/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

INSERT DATA {
  ex:battery ex:voltage {{ msg.voltage | rdf }} ;
             ex:percentage {{ msg.percentage | rdf }} ;
             ex:observedAt {{ msg.header.stamp | rdf }} .
}
{% endraw %}
```

The `rdf` filter produces a valid typed RDF literal. Prefer it over manually
adding quotes or datatypes: it correctly handles ROS numeric, boolean, time,
and supported geometry messages. See [ROS to RDF conversion](ros-to-rdf.md).

## Understand the execution model

For each message, Triplestar:

1. renders the template with `msg` bound to that message;
2. skips the update if the rendered result is empty;
3. executes the rendered text as a SPARQL update;
4. logs an error and continues listening if rendering or execution fails.

Templates are loaded when the lifecycle node is configured. After changing a
template in an installed package, rebuild if necessary and reconfigure or
restart the node so the file is loaded again.

## Choose stable resource identifiers

`INSERT DATA` adds triples; it does not replace older values. Reusing a stable
subject is appropriate for facts where multiple values are meaningful. For a
history of observations, generate a distinct IRI from fields in the message:

```jinja2
{% raw %}
PREFIX ex: <https://example.org/robot/>

INSERT DATA {
  <https://example.org/observation/{{ msg.header.stamp.sec }}-{{ msg.header.stamp.nanosec }}>
    a ex:BatteryObservation ;
    ex:value {{ msg.percentage | rdf }} ;
    ex:observedAt {{ msg.header.stamp | rdf }} .
}
{% endraw %}
```

Only interpolate trusted scalar fields into an IRI, and ensure the resulting
text is a legal IRI. For arbitrary strings, store them as literals with `rdf`
instead of placing them in SPARQL syntax directly.

## Use Jinja2 for conditional triples

Message fields can control which triples are emitted:

```jinja2
{% raw %}
PREFIX ex: <https://example.org/robot/>

INSERT DATA {
  ex:battery ex:percentage {{ msg.percentage | rdf }} .
  {% if msg.power_supply_status == 1 %}
  ex:battery ex:charging {{ true | rdf }} .
  {% endif %}
}
{% endraw %}
```

Keep logic modest. Templates are easiest to review when they describe RDF; move
complex transformations into a ROS node or a custom SPARQL function.

## Verify an insertion

Start the bringup package, publish one representative message, and call a small
configured `SELECT` query:

```bash
ros2 topic info /battery --verbose
ros2 topic pub --once /battery sensor_msgs/msg/BatteryState \
  "{voltage: 12.4, percentage: 0.82}"
ros2 triplestar query call battery_state
```

If no triple appears:

1. confirm the topic type and publisher count with `ros2 topic info`;
2. check that the template filename matches exactly;
3. inspect node logs for Jinja2 or SPARQL parser errors;
4. confirm every interpolated value uses `rdf` unless it intentionally emits
   SPARQL syntax;
5. inspect the rendered field path against `ros2 interface show MESSAGE_TYPE`.

## Related configuration

- [Configuration reference](../bringup_package/config-files.md) compares
  insertion, query-time topic, and query-time TF subscribers.
- [Learn RDF and SPARQL](learning-sparql.md) links to tutorials for writing the
  `INSERT`, `SELECT`, and graph patterns used by templates.
