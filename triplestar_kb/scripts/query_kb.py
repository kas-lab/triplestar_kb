#!/usr/bin/env python3
"""
CLI tool for querying the triplestar knowledge base.

This script provides a command-line interface to send SPARQL queries
to the triplestar_kb node via ROS 2 service calls.
"""

import argparse
import json
import sys
from pathlib import Path

import rclpy
from rclpy.node import Node
from triplestar_kb_msgs.srv import Query


class QueryKBClient(Node):
    """Client node for querying the knowledge base."""

    def __init__(self):
        super().__init__('query_kb_client')
        self.client = self.create_client(Query, '/triplestar_kb/query')

    def send_query(self, query_string: str) -> tuple[bool, str]:
        """
        Send a SPARQL query to the knowledge base.

        Args:
            query_string: The SPARQL query to execute

        Returns:
            Tuple of (success, result_json)
        """
        if not self.client.wait_for_service(timeout_sec=5.0):
            self.get_logger().error('Service not available')
            return False, ''

        request = Query.Request()
        request.query = query_string

        future = self.client.call_async(request)
        rclpy.spin_until_future_complete(self, future)

        if future.result() is not None:
            response = future.result()
            return response.success, response.result
        else:
            return False, ''


def main(args=None):
    """Main entry point for the query_kb CLI tool."""
    parser = argparse.ArgumentParser(description='Query the triplestar knowledge base with SPARQL')

    parser.add_argument(
        'filename',
        type=str,
        help='Path to file containing SPARQL query',
    )

    parser.add_argument(
        '--no-pretty',
        action='store_true',
        help='Disable pretty-printing of JSON results',
    )

    parsed_args = parser.parse_args()

    rclpy.init(args=args)

    try:
        query_string = Path(parsed_args.filename).read_text()

        client = QueryKBClient()
        success, result = client.send_query(query_string)

        if success and result:
            try:
                parsed = json.loads(result)
                if parsed_args.no_pretty:
                    print(json.dumps(parsed))
                else:
                    print(json.dumps(parsed, indent=2))
            except json.JSONDecodeError:
                print(result)
            return_code = 0
        else:
            print('Query failed', file=sys.stderr)
            return_code = 1

    except Exception as e:
        print(f'Error: {e}', file=sys.stderr)
        return_code = 1
    finally:
        try:
            client.destroy_node()
        except Exception:
            pass
        rclpy.shutdown()

    sys.exit(return_code)


if __name__ == '__main__':
    main()
