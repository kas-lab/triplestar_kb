import hashlib
from typing import Any, Dict, Optional, Union

from graphviz import Digraph
from pyoxigraph import BlankNode, Literal, NamedNode, Quad, Store, Triple


class RDFStarVisualizer:
    """A visualizer for RDF-star graphs using Graphviz."""

    # Standard prefixes commonly used in RDF
    STANDARD_PREFIXES = {
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#": "rdf",
        "http://www.w3.org/2000/01/rdf-schema#": "rdfs",
        "http://www.w3.org/2001/XMLSchema#": "xsd",
        "http://www.w3.org/2002/07/owl#": "owl",
        "http://xmlns.com/foaf/0.1/": "foaf",
        "http://purl.org/dc/elements/1.1/": "dc",
        "http://purl.org/dc/terms/": "dcterms",
        "http://www.w3.org/2004/02/skos/core#": "skos",
        "http://www.opengis.net/ont/geosparql#": "geo",
        "http://example.org/": "ex",
    }

    # Default style dictionaries for different node types
    DEFAULT_STYLES = {
        "named_node": {
            "shape": "ellipse",
            "style": "filled",
            "fillcolor": "lightblue",
            "fontname": "Arial",
            "fontsize": "12",
        },
        "literal": {
            "shape": "box",
            "style": "rounded,filled",
            "fillcolor": "lightgreen",
            "fontname": "Arial",
            "fontsize": "10",
        },
        "blank_node": {
            "shape": "diamond",
            "style": "filled",
            "fillcolor": "lightgray",
            "fontname": "Arial",
            "fontsize": "10",
        },
        "midpoint": {
            "shape": "point",
            "width": "0",
            "height": "0",
            "label": "",
            "fontsize": "8",
        },
    }

    def __init__(
        self,
        comment: str = "RDF-star Graph",
        format: str = "PNG",
        engine: str = "sfdp",
        styles: Optional[Dict[str, Dict[str, Any]]] = None,
        prefixes: Optional[Dict[str, str]] = None,
        show_legend: bool = True,
    ):
        """Initialize the visualizer.

        Args:
            comment: Comment for the graph
            format: Output format (png, svg, etc.)
            engine: Graphviz engine to use
            styles: Custom styles dictionary to override defaults
            prefixes: Custom prefixes dictionary (namespace -> prefix)
            show_legend: Whether to show a legend with used prefixes
        """
        self.dot = Digraph(comment=comment, format=format, engine=engine)
        self.styles = styles or self.DEFAULT_STYLES.copy()
        self._processed_nodes = set()

        # Merge standard prefixes with custom ones
        self.prefixes = self.STANDARD_PREFIXES.copy()
        if prefixes:
            self.prefixes.update(prefixes)

        # Track used prefixes for legend
        self.used_prefixes = set()
        self.show_legend = show_legend

    def safe_id(self, value: Union[str, Any]) -> str:
        """Generate a safe node ID from any value."""
        return "n" + hashlib.sha1(str(value).encode("utf-8")).hexdigest()

    def shorten_uri(self, uri: str) -> str:
        """Shorten a URI using known prefixes."""
        uri_str = str(uri)

        # Remove angle brackets if present
        if uri_str.startswith("<") and uri_str.endswith(">"):
            uri_str = uri_str[1:-1]

        # Try to find a matching prefix
        for namespace, prefix in self.prefixes.items():
            if uri_str.startswith(namespace):
                self.used_prefixes.add((prefix, namespace))
                return uri_str.replace(namespace, f"{prefix}:", 1)

        return uri_str

    def add_legend(self) -> None:
        """Add a legend subgraph showing used prefixes."""
        if not self.used_prefixes or not self.show_legend:
            return

        # Create legend subgraph
        with self.dot.subgraph(name="cluster_legend") as legend:
            legend.attr(label="Prefixes", fontsize="14", fontname="Arial Bold")
            legend.attr(style="filled", fillcolor="white", color="black")

            # Sort prefixes for consistent display
            sorted_prefixes = sorted(self.used_prefixes)

            # Create legend content as HTML table
            legend_rows = []
            for prefix, namespace in sorted_prefixes:
                escaped_ns = (
                    namespace.replace("&", "&amp;")
                    .replace("<", "&lt;")
                    .replace(">", "&gt;")
                )
                legend_rows.append(
                    f'<TR><TD ALIGN="LEFT"><B>{prefix}:</B></TD><TD ALIGN="LEFT">{escaped_ns}</TD></TR>'
                )

            legend_html = f"""<
            <TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">
                <TR><TD COLSPAN="2"><B>Prefixes</B></TD></TR>
                {"".join(legend_rows)}
            </TABLE>
            >"""

            legend.node("legend_content", legend_html, shape="none", margin="0")

    def literal_label(self, lit: Literal) -> str:
        """Generate label for literal nodes with optional datatype."""
        if lit.datatype:
            shortened_datatype = self.shorten_uri(str(lit.datatype))
            escaped_datatype = shortened_datatype.replace("<", "&lt;").replace(
                ">", "&gt;"
            )
            return f"""<
            <TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0">
                <TR><TD>"{str(lit.value)}"</TD></TR>
                <TR><TD><FONT POINT-SIZE="8" COLOR="darkgray">{escaped_datatype}</FONT></TD></TR>
            </TABLE>
            >"""
        else:
            return str(lit.value)

    def add_node_if_needed(
        self, node: Union[NamedNode, BlankNode, Literal, Triple], node_id: str
    ) -> bool:
        """Add a node if it hasn't been processed yet. Returns True if it's a star triple."""
        if node_id in self._processed_nodes:
            return isinstance(node, Triple)

        if isinstance(node, Triple):
            # For triples, we assume they already exist as midpoint nodes
            return True
        elif isinstance(node, NamedNode):
            shortened_label = self.shorten_uri(str(node))
            self.dot.node(node_id, shortened_label, **self.styles["named_node"])
        elif isinstance(node, BlankNode):
            self.dot.node(node_id, str(node), **self.styles["blank_node"])
        elif isinstance(node, Literal):
            self.dot.node(
                node_id, self.literal_label(node), **self.styles["literal"]
            )

        self._processed_nodes.add(node_id)
        return False

    def add_quad(self, quad: Quad) -> None:
        """Add a single quad to the visualization."""
        # Generate IDs
        subject_id = self.safe_id(str(quad.subject))
        midpoint_id = self.safe_id(str(quad.triple))
        object_id = self.safe_id(str(quad.object))

        # Check if this involves star triples
        subject_is_star = self.add_node_if_needed(quad.subject, subject_id)
        object_is_star = self.add_node_if_needed(quad.object, object_id)
        star_triple = subject_is_star or object_is_star

        # Add the midpoint node with shortened predicate label
        predicate_label = self.shorten_uri(str(quad.predicate))
        self.dot.node(
            midpoint_id, xlabel=predicate_label, **self.styles["midpoint"]
        )

        # Add edges based on whether it's a star triple
        if star_triple:
            self.dot.edge(
                subject_id,
                midpoint_id,
                arrowhead="none",
                style="dashed",
                color="gray",
            )
            self.dot.edge(midpoint_id, object_id, style="dashed", color="gray")
        else:
            self.dot.edge(subject_id, midpoint_id, arrowhead="none")
            self.dot.edge(midpoint_id, object_id)

    def add_quads(self, quads) -> None:
        """Add multiple quads to the visualization."""
        for quad in quads:
            self.add_quad(quad)

    def get_source(self) -> str:
        """Get the DOT source code."""
        return self.dot.source

    def generate_visualization(
        self,
        store: Store,  # Use a read-only pyoxigraph store
        query: Optional[str] = None,
    ) -> Optional[bytes]:
        if query:
            # Execute CONSTRUCT query through the interface
            results = store.query(query)
            if not hasattr(results, "__iter__"):
                raise TypeError("Query results must be iterable")

            for quad in results:
                self.add_quad(quad)
        else:
            # Add all quads from the shared store
            for quad in store:
                self.add_quad(quad)

        # Render and return bytes
        return self.dot.pipe()
