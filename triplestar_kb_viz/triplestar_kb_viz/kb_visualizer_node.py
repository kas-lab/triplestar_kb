from pathlib import Path
from typing import Optional

import rclpy
from PIL import Image as PILImage
from pyoxigraph import Store
from rclpy.executors import ExternalShutdownException
from rclpy.node import Node
from sensor_msgs.msg import Image
from triplestar_kb_msgs.srv import SetVizQuery

from .kb_visualizer import RDFStarVisualizer


class KBVisualizerNode(Node):
    def __init__(self):
        super().__init__("kb_visualizer")

        self.visualizer = RDFStarVisualizer()
        self.store_path = None
        self.store = None

        # Query configuration
        self.current_query = None  # None means visualize entire store
        self.update_rate = 5.0
        self.auto_refresh = True
        self.timer = None

        # Declare parameter to get the KB store path
        self.declare_parameter("store_path", rclpy.Parameter.Type.STRING)

        self._get_store()

        # Publishers
        self.image_pub = self.create_publisher(
            Image, "~/graph_visualization/image", 10
        )

        # Services
        self.set_viz_query_srv = self.create_service(
            SetVizQuery, "set_viz_query", self._handle_set_viz_query
        )

        self.get_logger().info("Triplestar KB Visualizer node created")

        # Initialize timer with default update rate
        self._create_visualization_timer()

    def _create_visualization_timer(self):
        """Create or recreate the visualization timer with current update rate."""
        if self.timer is not None:
            self.destroy_timer(self.timer)

        if self.auto_refresh and self.update_rate > 0:
            self.timer = self.create_timer(
                self.update_rate,
                lambda: self.generate_and_publish_visualization(
                    self.current_query
                ),
            )

    def _handle_set_viz_query(
        self, request: SetVizQuery.Request, response: SetVizQuery.Response
    ):
        """Handle SetVizQuery service requests."""
        try:
            # Validate query - now allow empty query to visualize entire store
            if request.query is None or request.query.strip() == "":
                # Empty query means visualize entire store
                self.current_query = None
            else:
                self.current_query = request.query

            # Validate update rate
            if request.update_rate <= 0:
                response.success = False
                response.message = "Update rate must be positive"
                self.get_logger().warn(response.message)
                return response

            # Update configuration
            self.update_rate = request.update_rate
            self.auto_refresh = request.auto_refresh

            # Recreate timer with new settings
            self._create_visualization_timer()

            response.success = True
            query_status = (
                "entire store" if self.current_query is None else "custom query"
            )
            response.message = f"Visualization query updated. Visualizing: {self.current_query}, Auto-refresh: {self.auto_refresh}, Update rate: {self.update_rate}s"
            self.get_logger().info(response.message)

            # Publish visualization immediately with new query
            self.generate_and_publish_visualization(self.current_query)

        except Exception as e:
            response.success = False
            response.message = f"Error updating query: {str(e)}"
            self.get_logger().error(response.message)

        return response

    def _get_store(self):
        param = self.get_parameter("store_path")

        if param.type_ == rclpy.Parameter.Type.NOT_SET or not param.value:
            self.get_logger().warning(
                "store path param not set for visualizer node"
            )
            return {}

        self.store_path = Path(param.value)

        try:
            if not self.store_path.exists():
                self.get_logger().warning(
                    f"Store path {self.store_path} does not exist. Cannot open a readonly sotre."
                )
            self.store = Store.read_only(str(self.store_path))

        except Exception as e:
            self.get_logger().error(
                f"Failed to access readonly store at {self.store_path}: {e}"
            )

    def generate_and_publish_visualization(self, query: Optional[str] = None):
        if not self.store:
            self.get_logger().warn("No store available for visualiztion")
            return

        image_data = self.visualizer.generate_visualization(
            store=self.store, query=query
        )

        if image_data:
            try:
                import tempfile

                # temporary file
                with tempfile.NamedTemporaryFile(
                    suffix=".png", delete=False
                ) as temp_file:
                    temp_file.write(image_data)
                    temp_path = temp_file.name

                # Use PIL to open the image from bytes
                pil_image = PILImage.open(temp_path)

                # Convert PIL image to RGB mode if necessary
                if pil_image.mode != "RGB":
                    pil_image = pil_image.convert("RGB")
                width, height = pil_image.size
                data = pil_image.tobytes()

                msg = Image()
                msg.height = height
                msg.width = width
                msg.encoding = "rgb8"
                msg.is_bigendian = False
                msg.step = width * 3  # 3 bytes per pixel for RGB
                msg.data = data

                self.image_pub.publish(msg)
                query_status = (
                    "entire store" if query is None else "query result"
                )
                self.get_logger().info(
                    f"Published visualization of {query_status}"
                )
            except Exception as e:
                self.get_logger().error(
                    f"Failed to convert and publish image: {e}"
                )


def main(args=None):
    rclpy.init(args=args)

    node = KBVisualizerNode()

    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    except ExternalShutdownException:
        exit(1)
    finally:
        node.destroy_node()
        rclpy.try_shutdown()
