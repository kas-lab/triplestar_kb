#!/usr/bin/env python3
"""
Command-line interface for RDF* visualization.

This tool provides a standalone CLI for visualizing RDF graphs without needing ROS.
"""

import argparse
import sys
from pathlib import Path
from typing import Optional

from triplestar_kb_viz.core import RDFLoader, RDFStarVisualizer


def infer_format_from_extension(path: Path) -> str:
    """Infer RDF format from file extension."""
    ext = path.suffix.lower()
    format_map = {
        '.ttl': 'turtle',
        '.turtle': 'turtle',
        '.nt': 'ntriples',
        '.ntriples': 'ntriples',
        '.rdf': 'rdfxml',
        '.xml': 'rdfxml',
        '.n3': 'n3',
        '.nq': 'nquads',
        '.nquads': 'nquads',
        '.trig': 'trig',
    }
    return format_map.get(ext, 'turtle')


def infer_output_format(path: Path) -> str:
    """Infer output format from file extension."""
    ext = path.suffix.lower()
    if ext in ['.png', '.svg', '.pdf', '.jpg', '.jpeg']:
        return ext[1:]  # Remove the dot
    return 'png'  # Default to PNG


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Visualize RDF* graphs using Graphviz',
        epilog="""
Examples:
  %(prog)s data.ttl -o output.svg
  %(prog)s data.ttl --query query.rq -o result.png
  %(prog)s --store /path/to/store -o graph.pdf
  %(prog)s data.nt --format ntriples --engine dot -o tree.svg
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Input options (mutually exclusive)
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        'file',
        nargs='?',
        type=Path,
        help='RDF file to visualize (TTL, N-Triples, RDF/XML, etc.)',
    )
    input_group.add_argument(
        '--store',
        type=Path,
        help='Path to pyoxigraph store directory',
    )

    # Query options
    query_group = parser.add_mutually_exclusive_group()
    query_group.add_argument(
        '--query',
        type=Path,
        help='SPARQL CONSTRUCT query file to filter/transform data',
    )
    query_group.add_argument(
        '--query-string',
        type=str,
        help='SPARQL CONSTRUCT query as string',
    )

    # Input format
    parser.add_argument(
        '--format',
        type=str,
        choices=['turtle', 'ntriples', 'rdfxml', 'n3', 'nquads', 'trig'],
        help='Input RDF format (default: infer from file extension)',
    )

    # Output options
    parser.add_argument(
        '-o',
        '--output',
        type=Path,
        required=True,
        help='Output file (extension determines format: .png, .svg, .pdf)',
    )
    parser.add_argument(
        '--output-format',
        choices=['png', 'svg', 'pdf', 'jpg'],
        help='Output format (default: infer from output filename)',
    )

    # Visualization options
    parser.add_argument(
        '--engine',
        default='sfdp',
        choices=['dot', 'neato', 'fdp', 'sfdp', 'circo', 'twopi'],
        help='Graphviz layout engine (default: sfdp)',
    )
    parser.add_argument(
        '--no-legend',
        action='store_true',
        help='Hide the prefix legend',
    )
    parser.add_argument(
        '--comment',
        default='RDF* Graph',
        help='Graph comment/title',
    )

    # Verbosity
    parser.add_argument(
        '-v',
        '--verbose',
        action='store_true',
        help='Verbose output',
    )

    args = parser.parse_args()

    # Validate inputs
    if args.file and not args.file.exists():
        print(f'Error: File not found: {args.file}', file=sys.stderr)
        return 1

    if args.store and not args.store.exists():
        print(f'Error: Store not found: {args.store}', file=sys.stderr)
        return 1

    if args.query and not args.query.exists():
        print(f'Error: Query file not found: {args.query}', file=sys.stderr)
        return 1

    try:
        # Load RDF data
        if args.verbose:
            print('Loading RDF data...')

        if args.file:
            # Infer format if not specified
            rdf_format = args.format or infer_format_from_extension(args.file)
            if args.verbose:
                print(f'  Loading from file: {args.file} (format: {rdf_format})')
            store = RDFLoader.from_file(args.file, format=rdf_format)
        else:  # args.store
            if args.verbose:
                print(f'  Loading from store: {args.store}')
            store = RDFLoader.from_store(args.store, read_only=True)

        # Count quads for feedback
        quad_count = sum(1 for _ in store)
        if args.verbose:
            print(f'  Loaded {quad_count} quads')

        # Load query if provided
        query: Optional[str] = None
        if args.query:
            if args.verbose:
                print(f'Loading query from: {args.query}')
            query = RDFLoader.load_query_from_file(args.query)
        elif args.query_string:
            if args.verbose:
                print('Using query string')
            query = args.query_string

        # Determine output format
        output_format = args.output_format or infer_output_format(args.output)
        if args.verbose:
            print(f'Output format: {output_format}')
            print(f'Layout engine: {args.engine}')

        # Create visualizer
        visualizer = RDFStarVisualizer(
            comment=args.comment,
            format=output_format.upper(),
            engine=args.engine,
            show_legend=not args.no_legend,
        )

        # Generate visualization
        if args.verbose:
            print('Generating visualization...')

        image_data = visualizer.generate_visualization(store=store, query=query)

        if not image_data:
            print('Error: No visualization data generated', file=sys.stderr)
            return 1

        # Write output
        if args.verbose:
            print(f'Writing output to: {args.output}')

        args.output.parent.mkdir(parents=True, exist_ok=True)
        with open(args.output, 'wb') as f:
            f.write(image_data)

        if args.verbose:
            print(f'✓ Visualization saved ({len(image_data)} bytes)')

        return 0

    except FileNotFoundError as e:
        print(f'Error: {e}', file=sys.stderr)
        return 1
    except Exception as e:
        print(f'Error: {e}', file=sys.stderr)
        if args.verbose:
            import traceback

            traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
