Each TriplestarKB instance uses a custom bringup package for its configuration.
Generate one with `ros2 triplestar bringup new` rather than copying a package by hand.

## Folder Structure

TriplestarKB expects custom bringup packages to keep the generated directory structure, including `config/triplestar.yaml`.

## Config Files

All settings live in `config/triplestar.yaml`. It contains:

- `knowledge_base` settings (`store_path`, `base_iri`, `clear_on_startup`, and `preload_files`);
- lists of insertion, query-time topic, and query-time TF subscribers;
- a list of dynamically typed insertion service mirrors; and
- a list of query services.

See the [configuration reference](config-files.md) for the schema and examples.

## Queries

Put [SPARQL](https://www.w3.org/TR/sparql12-query/) queries in this folder.
These queries can be exposed as services through the `query_services` list in `config/triplestar.yaml`.

## Preload

Put ttl files with information you want preloaded into the knowledge base here.

**WARNING**: if you are using triple annotations (The new feature in RDF 1.2), make sure to use [explicit reifiers](https://www.w3.org/TR/rdf12-turtle/#ex-reified-triple-with-reifier). If this is not done the KB will create new blank nodes for the reifier on each startup, causing unwanted duplication of information.

## Templates

Put your [SPARQL](https://www.w3.org/TR/sparql12-query/) insertion templates in this folder.
You can use [jinja2](https://jinja.palletsprojects.com/en/stable/) syntax (so fields are surrounded by double curly braces).
Converting `ROS` message values to `RDF` values can be done using the `rdf` filter function.

For instance, the following will convert a ROS pose to a point.

```jinja2
{{ msg.pose | rdf }}
```
