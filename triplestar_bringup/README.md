# TriplestarKB Bringup Package

This is an example bringup package for TriplestarKB.
For your own workspace, copy this package into your src folder and create a custom configuration.

## Folder Structure

TriplestarKB assumes your custom config package to follow the directory structure of this package, so yaml config files in the config folder, so dont change the folder names.

## Config Files

There are three config files: `kb_params.yaml`, `query_services.yaml` and `subscribers.yaml`.

### `kb_params.yaml`

This config file contains the path where `oxigraph` will store data, which files to preload into the KB and what the name of the config package is. Specifying the name of your custom config package will allow the KB to access the share directory of that package, and find your custom preload, query and template files.

### `query_services.yaml`

This file is used to spin up query services using the files in your `queries` folder.

### `subscribers.yaml`

This file is used for configuring the topics the KB subscribes to to pull in data.
The KB pulls in data using [query time subscribers](../README.md#query-time-subscribers) [query time tf subscribers](../README.md#query-time-tf-subscribers) and [insertion subscribers].

## Queries

Put [SPARQL](https://www.w3.org/TR/sparql12-query/) queries in this folder.
Those queries can then be used to setup query services using the `query_services.yaml` config file.

## Preload

Put ttl files with information you want preloaded into the knowledge base here.

**WARNING**: if you are using triple annotations (The new feature in RDF 1.2), make sure to use [explicit reifiers](https://www.w3.org/TR/rdf12-turtle/#ex-reified-triple-with-reifier). If this is not done the KB will create new blank nodes for the reifier on each startup, causing unwanted duplication of information.

## Templates

Put your [SPARQL](https://www.w3.org/TR/sparql12-query/) insertion templates in this folder.
You can use [jinja2](https://jinja.palletsprojects.com/en/stable/) syntax (so fields are surrounded by double curly braces).
Converting `ROS` message values to `RDF` values can be done using the `rdf` filter function.

For instance, the following will convert a ROS pose to a point (see [datatype conversions](../README.md#ros-to-rdf-conversions)).

```jinja2
{{ msg.pose | rdf }}
```
