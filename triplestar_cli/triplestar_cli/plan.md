# Triplestar CLI

To help triplestar users get more convenience at the command line, we are making a CLI that extends the ros2 cli.

## Bringup

Create a new triplestar bringup package with
```bash
ros2 triplestar bringup new
```

## Queries

Users will be able to inspect and call available file-based query services, such as `/triplestar/query/count_triples`.

```bash
ros2 triplestar query list
```
to list triples

```bash
ros2 triplestar query call {query_name}
```
to call a query (lets get tab completion working)

```bash
ros2 triplestar query info
```
to show info about the query, preferably also outputting the sparql file to the terminal

```bash
```

## Functions

## Lifecycle
Triplestar is a lifecycle node.
That means that it goes throug different phases.

If a topic that triplestar gets data from is not up at startup time, it will not be able to create a subscriber to that topic.
For that reason, if maybe you start a node triplstar needs info from latre, we may want to unconfigure and reconfigure the KB.

> This points to an important issue; loading of preload data and optional clearing of the graph should be done at startup, while activating the dynamic subscribers can be done on configure.

```bash
ros2 triplestar ??? configure
```

or reconfigure i guess would undo that stuff.
