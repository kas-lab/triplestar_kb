from functools import partial
from pathlib import Path
from typing import Optional

import rclpy
import rclpy.executors
import yaml
from pyoxigraph import NamedNode
from rclpy.lifecycle import (
    LifecycleNode,
    LifecycleState,
    TransitionCallbackReturn,
)
from triplestar_kb_msgs.srv import Query

from .kb_interface import TriplestarKBInterface
from .msg_to_rdf import ros_msg_to_literal
from .query_time_subscriber import QueryTimeSubscriber, QueryTimeTFSubscriber

XSD = "http://www.w3.org/2001/XMLSchema#"
EX = "http://example.org/"


class RosTriplestarKBInterface(LifecycleNode):
    """
    A ROS2 lifecycle node for managing a triplestar knowledge base using pyoxigraph.

    This node handles RDF data storage and retrieval with support for preloading
    Turtle (.ttl) files during configuration.
    """

    def __init__(self):
        super().__init__("triplestar_kb")

        # Core KB components
        self.kb: Optional[TriplestarKBInterface] = None
        self.query_time_subs = {}
        self.tf_subscriber = None

        # Declare parameters
        self._declare_parameters()

        self.get_logger().info("Triplestar KB node created")

    def _declare_parameters(self):
        """Declare all node parameters."""
        # Core parameters
        self.declare_parameter("store_path", rclpy.Parameter.Type.STRING)
        self.declare_parameter("preload_path", rclpy.Parameter.Type.STRING)
        self.declare_parameter("preload_files", rclpy.Parameter.Type.STRING_ARRAY)
        self.declare_parameter("query_time_subscriptions", rclpy.Parameter.Type.STRING)
        self.declare_parameter("query_time_tf_subscriptions", rclpy.Parameter.Type.STRING)

    def on_configure(self, state: LifecycleState) -> TransitionCallbackReturn:
        """Initialize the RDF store and load preload files."""
        self.get_logger().info("Configuring KB node...")

        store_path = Path(self.get_parameter("store_path").value)

        # Initialize the kb interface
        self.kb = TriplestarKBInterface(store_path=store_path, logger=self.get_logger())

        # Load preload files
        if not self._preload_files():
            self.get_logger().error("Failed to preload files")
            return TransitionCallbackReturn.ERROR

        # Initialize query-time subscriptions
        self._create_query_time_subscriptions()

        self.get_logger().info("KB node configured successfully")
        return TransitionCallbackReturn.SUCCESS

    def on_cleanup(self, state: LifecycleState) -> TransitionCallbackReturn:
        """Clean up resources."""
        self.get_logger().info("Cleaning up KB node...")

        if self.kb:
            self.kb.close()
            self.kb = None

        self.get_logger().info("KB node cleaned up")
        return TransitionCallbackReturn.SUCCESS

    def on_activate(self, state: LifecycleState) -> TransitionCallbackReturn:
        """Activate the KB node for serving."""
        self.get_logger().info("Activating KB node...")

        # Create query service
        self.query_service = self.create_service(
            Query, f"{self.get_name()}/query", self.query_callback
        )

        result = super().on_activate(state)

        if result == TransitionCallbackReturn.SUCCESS:
            self.get_logger().info("KB node activated and ready to serve")
        else:
            self.get_logger().error("Failed to activate KB node")

        return result

    def on_deactivate(self, state: LifecycleState) -> TransitionCallbackReturn:
        """Deactivate the KB node but keep data in memory."""
        self.get_logger().info("Deactivating KB node...")

        result = super().on_deactivate(state)

        if result == TransitionCallbackReturn.SUCCESS:
            self.get_logger().info("KB node deactivated")
        else:
            self.get_logger().error("Failed to deactivate KB node")

        return result

    def on_shutdown(self, state: LifecycleState) -> TransitionCallbackReturn:
        """Final shutdown and cleanup."""
        self.get_logger().info("Shutting down KB node...")
        return TransitionCallbackReturn.SUCCESS

    def _create_query_time_subscriptions(self):
        """Create subscriptions for query-time data injection."""
        # Load regular topic subscriptions
        query_time_subscriptions = self._load_subscription_config(
            "query_time_subscriptions", "No query time subscriptions specified"
        )

        # Load TF subscriptions
        query_time_tf_subscriptions = self._load_subscription_config(
            "query_time_tf_subscriptions",
            "No query time TF subscriptions specified",
        )

        # Create regular topic subscribers
        for name, values in query_time_subscriptions.items():
            self.query_time_subs[name] = QueryTimeSubscriber(node=self, topic_name=values["topic"])

        # Create TF subscriber if needed
        if query_time_tf_subscriptions:
            self.tf_subscriber = QueryTimeTFSubscriber(node=self)

        # Register custom functions for topic subscribers
        for name, sub in self.query_time_subs.items():
            func = partial(lambda s: ros_msg_to_literal(s.get_latest()), sub)
            self.kb._add_custom_function(NamedNode(EX + name), func)

        # Register custom functions for TF subscriptions
        for name, values in query_time_tf_subscriptions.items():
            from_frame = values["from_frame"]
            to_frame = values["to_frame"]
            func = partial(
                lambda tf_sub, from_f, to_f: ros_msg_to_literal(tf_sub.get_latest(from_f, to_f)),
                self.tf_subscriber,
                from_frame,
                to_frame,
            )
            self.kb._add_custom_function(NamedNode(EX + name), func)

    def _load_subscription_config(self, param_name: str, empty_msg: str) -> dict:
        """Load subscription configuration from parameter."""
        try:
            param = self.get_parameter(param_name)
            if param.type_ == rclpy.Parameter.Type.NOT_SET or not param.value:
                self.get_logger().info(empty_msg)
                return {}

            subscriptions = yaml.safe_load(param.value) or {}
            self.get_logger().info(f"Loaded subscriptions for '{param_name}': {subscriptions}")
            return subscriptions

        except Exception as e:
            self.get_logger().warning(f"Failed to get parameter '{param_name}': {e}")
            return {}

    def _preload_files(self) -> bool:
        """Preload TTL files from the specified directory."""
        preload_path_param = self.get_parameter("preload_path").value

        if not preload_path_param:
            self.get_logger().warn("Preload path parameter is empty, skipping preload")
            return True

        preload_path = Path(preload_path_param)

        if not preload_path.exists():
            self.get_logger().warn(f"Preload path {preload_path} does not exist")
            return False

        if not preload_path.is_dir():
            self.get_logger().warn(f"Preload path {preload_path} is not a directory")
            return False

        self.get_logger().info(f"Preloading files from {preload_path}")

        # Find all .ttl files
        file_paths = [f for f in preload_path.iterdir() if f.is_file() and f.suffix == ".ttl"]

        if not file_paths:
            self.get_logger().warn(f"No .ttl files found in {preload_path}")
            return False

        self.get_logger().info(f"Found {len(file_paths)} .ttl files to preload")

        # Load files
        loaded_count = self.kb.load_files(file_paths)

        if loaded_count == 0:
            self.get_logger().warn(f"No files were successfully loaded from {preload_path}")
            return False

        self.get_logger().info(f"Successfully preloaded {loaded_count} files from {preload_path}")
        return True

    def query_callback(self, request: Query.Request, response: Query.Response) -> Query.Response:
        """Handle a query request."""
        self.get_logger().debug(f"Received query request: {request}")

        response.result = self.kb.query_json(request.query)
        response.success = response.result != ""

        return response
