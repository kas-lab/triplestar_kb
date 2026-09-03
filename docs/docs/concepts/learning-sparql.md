---
icon: lucide/book-open
---

# Learn RDF and SPARQL

Triplestar stores an RDF graph and uses SPARQL both to read it and to insert or
update data. You do not need the whole semantic-web stack before starting.

## Recommended path

1. Read the [RDF 1.2 Primer](https://www.w3.org/TR/rdf12-primer/) for resources,
   IRIs, literals, triples, and graphs.
2. Work through the [Apache Jena SPARQL tutorial](https://jena.apache.org/tutorials/sparql.html)
   for hands-on `SELECT`, graph patterns, filters, optional data, and named
   graphs. Its examples apply to Triplestar even though the runtime is not Jena.
3. Use the [SPARQL 1.1 Query Language](https://www.w3.org/TR/sparql11-query/) as
   the precise reference when syntax or behavior is unclear.
4. Read [SPARQL 1.1 Update](https://www.w3.org/TR/sparql11-update/) when writing
   `INSERT`, `DELETE`, or combined updates for insertion templates.

## What to learn first

For querying Triplestar, focus on:

- `PREFIX` declarations and IRIs;
- triple patterns and variables;
- `SELECT`, `ASK`, and `CONSTRUCT`;
- `FILTER`, `OPTIONAL`, and `BIND`;
- property paths and aggregates such as `COUNT`;
- typed literals and named graphs.

For [insertion templates](insertion-templates.md), add:

- `INSERT DATA` for adding concrete triples;
- `DELETE`/`INSERT ... WHERE` for replacing matched state;
- valid RDF terms, especially the difference between an IRI and a literal.

## Triplestar-specific extensions

After the standard syntax, learn the project-specific pieces:

- the `rdf` Jinja2 filter converts ROS fields into typed RDF literals;
- `qt:` functions expose query-time ROS topic and TF values;
- `fn:` functions call Python functions registered by a bringup package;
- optional OWL 2 RL reasoning can add inferred triples before a query.

These extensions compose with normal SPARQL. Keeping the graph model and most
queries standards-based makes them easier to test outside the robot stack.
