from pathlib import Path
from typing import Optional

import rclpy
from PIL import Image as PILImage
from pyoxigraph import Store
from rclpy.executors import ExternalShutdownException
from rclpy.node import Node
from sensor_msgs.msg import Image

from .kb_visualizer import RDFStarVisualizer


class KBVisualizerNode(Node):
    def __init__(self):
        super().__init__("kb_visualizer")

        self.visualizer = RDFStarVisualizer()
        self.store_path = None
        self.store = None

        # Declare parameter to get the KB store path
        self.declare_parameter("store_path", rclpy.Parameter.Type.STRING)

        self._get_store()

        # Publishers
        self.image_pub = self.create_publisher(Image, "~/graph_visualization/image", 10)

        self.get_logger().info("Triplestar KB Visualizer node created")

        self.timer = self.create_timer(
            5.0,
            lambda: self.generate_and_publish_visualization(
                # 'CONSTRUCT { ?subject ?predicate ?object } WHERE { ?subject ?predicate ?object . FILTER(CONTAINS(STR(?subject), "Robot1") || CONTAINS(STR(?predicate), "Robot1") || CONTAINS(STR(?object), "Robot1")) }'
            ),
        )

    def _get_store(self):
        param = self.get_parameter("store_path")

        if param.type_ == rclpy.Parameter.Type.NOT_SET or not param.value:
            self.get_logger().warning("store path param not set for visualizer node")
            return {}

        self.store_path = Path(param.value)

        try:
            if not self.store_path.exists():
                self.get_logger().warning(
                    f"Store path {self.store_path} does not exist. Cannot open a readonly sotre."
                )
            self.store = Store.read_only(str(self.store_path))

        except Exception as e:
            self.get_logger().error(f"Failed to access readonly store at {self.store_path}: {e}")

    def generate_and_publish_visualization(self, query: Optional[str] = None):
        if not self.store:
            self.get_logger().warn("No store available for visualiztion")
            return

        image_data = self.visualizer.generate_visualization(store=self.store, query=query)

        if image_data:
            try:
                import tempfile

                # temporary file
                with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as temp_file:
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
                self.get_logger().info("Published visualization")
            except Exception as e:
                self.get_logger().error(f"Failed to convert and publish image: {e}")


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
