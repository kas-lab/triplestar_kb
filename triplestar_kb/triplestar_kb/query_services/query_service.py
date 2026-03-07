from pathlib import Path

from rclpy.lifecycle import LifecycleNode
from rclpy.node import Node
from triplestar_kb.ros_kb_interface import TriplestarKBInterface
from triplestar_kb_msgs.srv import Query

"""
the query service sets up a service, and takes in a query file.
when called it calls the query service with that query file and then returns the json data.
"""


class QueryService:
    def __init__(
        self,
        node: Node | LifecycleNode,
        name: str,
        query_file: Path,
        kb: TriplestarKBInterface,
    ):
        self.name = name
        if not query_file.exists():
            raise FileNotFoundError(f'Query file {query_file} does not exist')
        self.query_file = query_file
        self.kb = kb

        self._query_service = node.create_service(
            srv_name=name,
            srv_type=Query,
            callback=self._callback,
        )

    def _callback(
        self,
        request: Query.Request,
        response: Query.Response,
    ) -> Query.Response:
        with open(self.query_file, 'r') as f:
            query = f.read()
        response.result = self.kb.query_json(query=query)
        return response
