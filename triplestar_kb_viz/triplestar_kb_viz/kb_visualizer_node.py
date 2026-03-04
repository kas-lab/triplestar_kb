from pathlib import Path
from typing import Optional

import rclpy
from PIL import Image as PILImage
from pyoxigraph import Store
from rclpy.executors import ExternalShutdownException
from rclpy.node import Node
from sensor_msgs.msg import Image
from triplestar_kb_msgs.srv import SetVizQuery

from triplestar_kb_viz.core import RDFStarVisualizer


class KBVisualizerNode(Node):
    def __init__(self):
        super().__init__('kb_visualizer')

        self.visualizer = RDFStarVisualizer()
        self.store_path = None
        self.store = None

        # Query configuration
        self.current_query = None  # None means visualize entire store
        self.update_rate = None  # None means no periodic updates
        self.timer = None

        # Declare parameter to get the KB store path
        self.declare_parameter('store_path', rclpy.Parameter.Type.STRING)

        self._get_store()

        # Publishers
        self.image_pub = self.create_publisher(Image, '~/graph_visualization/image', 10)

        # Services
        self.set_viz_query_srv = self.create_service(
            SetVizQuery, 'set_viz_query', self._handle_set_viz_query
        )

        self.get_logger().info('Triplestar KB Visualizer node created')

    def _create_visualization_timer(self):
        """Create or recreate the visualization timer with current update rate."""
        if self.timer is not None:
            self.get_logger().info('Destroying existing visualization timer')
            self.destroy_timer(self.timer)

        if self.update_rate is not None and self.update_rate > 0:
            self.get_logger().info(f'Creating timer with update rate: {self.update_rate}s')
            self.timer = self.create_timer(
                self.update_rate,
                lambda: self.generate_and_publish_visualization(self.current_query),
            )
            self.get_logger().info('Timer created successfully')
        else:
            self.get_logger().info('No timer created (update_rate is None or <= 0)')

    def _handle_set_viz_query(self, request: SetVizQuery.Request, response: SetVizQuery.Response):
        """Handle SetVizQuery service requests."""
        self.get_logger().info('=== SetVizQuery service called ===')
        self.get_logger().info(
            f'Received query (length: {len(request.query) if request.query else 0})'
        )
        self.get_logger().info(f'Received update_rate: {request.update_rate}')

        try:
            # Validate query - now allow empty query to visualize entire store
            if request.query is None or request.query.strip() == '':
                # Empty query means visualize entire store
                self.current_query = None
                self.get_logger().info('Empty query received - will visualize entire store')
            else:
                self.current_query = request.query
                self.get_logger().info(
                    f'Query set to: {self.current_query[:100]}...'
                )  # Log first 100 chars

            # Update configuration
            # If update_rate is not set (0 or negative), use default of 1 Hz
            if request.update_rate > 0:
                self.update_rate = request.update_rate
            else:
                self.update_rate = 1.0

            self.get_logger().info(f'Update rate set to: {self.update_rate}s')

            # Recreate timer with new settings
            self.get_logger().info('Creating visualization timer...')
            self._create_visualization_timer()

            response.success = True
            response.message = f'Visualization query updated. Update rate: {self.update_rate}s'
            self.get_logger().info(response.message)

            # Publish visualization immediately with new query
            self.get_logger().info('Publishing visualization immediately...')
            self.generate_and_publish_visualization(self.current_query)
            self.get_logger().info('=== SetVizQuery service completed successfully ===')

        except Exception as e:
            response.success = False
            response.message = f'Error updating query: {str(e)}'
            self.get_logger().error('=== SetVizQuery service FAILED ===')
            self.get_logger().error(response.message)
            import traceback

            self.get_logger().error(f'Traceback: {traceback.format_exc()}')

        return response

    def _get_store(self):
        param = self.get_parameter('store_path')

        if param.type_ == rclpy.Parameter.Type.NOT_SET or not param.value:
            self.get_logger().warning('store path param not set for visualizer node')
            return {}

        self.store_path = Path(param.value)

        try:
            if not self.store_path.exists():
                self.get_logger().warning(
                    f'Store path {self.store_path} does not exist. Cannot open a readonly sotre.'
                )
            self.store = Store.read_only(str(self.store_path))

        except Exception as e:
            self.get_logger().error(f'Failed to access readonly store at {self.store_path}: {e}')

    def generate_and_publish_visualization(self, query: Optional[str] = None):
        self.get_logger().info('--- Generating visualization ---')

        if not self.store:
            self.get_logger().warn('No store available for visualization')
            return

        self.get_logger().info(f'Store path: {self.store_path}')
        query_status = 'entire store' if query is None else 'custom query'
        self.get_logger().info(f'Visualizing: {query_status}')

        try:
            image_data = self.visualizer.generate_visualization(store=self.store, query=query)
            self.get_logger().info(
                f'Generated image data: {len(image_data) if image_data else 0} bytes'
            )
        except Exception as e:
            self.get_logger().error(f'Failed to generate visualization: {e}')
            import traceback

            self.get_logger().error(f'Traceback: {traceback.format_exc()}')
            return

        if image_data:
            try:
                import tempfile

                # temporary file
                with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as temp_file:
                    temp_file.write(image_data)
                    temp_path = temp_file.name

                # Use PIL to open the image from bytes
                pil_image = PILImage.open(temp_path)

                # Convert PIL image to RGB mode if necessary
                if pil_image.mode != 'RGB':
                    pil_image = pil_image.convert('RGB')
                width, height = pil_image.size
                data = pil_image.tobytes()

                msg = Image()
                msg.height = height
                msg.width = width
                msg.encoding = 'rgb8'
                msg.is_bigendian = False
                msg.step = width * 3  # 3 bytes per pixel for RGB
                msg.data = data

                self.image_pub.publish(msg)
                self.get_logger().info(
                    f'Published visualization of {query_status} ({width}x{height})'
                )
            except Exception as e:
                self.get_logger().error(f'Failed to convert and publish image: {e}')
                import traceback

                self.get_logger().error(f'Traceback: {traceback.format_exc()}')
        else:
            self.get_logger().warn('No image data generated')


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
