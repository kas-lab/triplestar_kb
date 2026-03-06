from pathlib import Path
from typing import Optional

import rclpy
import yaml
from ament_index_python import get_package_share_directory
from rclpy.lifecycle import (
    LifecycleNode,
    LifecycleState,
    TransitionCallbackReturn,
)
from triplestar_kb_msgs.srv import Query

from triplestar_kb.kb_interface import TriplestarKBInterface
from triplestar_kb.subscriptions.subscriber_manager import SubscriberManager


class RosTriplestarKBInterface(LifecycleNode):
    """
    A ROS2 lifecycle node for managing a triplestar knowledge base using pyoxigraph.
    """

    def __init__(self):
        super().__init__('triplestar_kb')

        self.kb: Optional[TriplestarKBInterface] = None
        self.subscriber_manager: Optional[SubscriberManager] = None

        self._declare_parameters()

        self.get_logger().info('Triplestar KB node created')

    def _declare_parameters(self):
        """Declare all node parameters with their default values."""
        bringup_share = get_package_share_directory('triplestar_kb_bringup')

        self.declare_parameter('store_path', str(Path(bringup_share) / 'store'))
        self.declare_parameter('templates_dir', str(Path(bringup_share) / 'templates'))
        self.declare_parameter('queries_dir', str(Path(bringup_share) / 'queries'))
        self.declare_parameter('preload_dir', str(Path(bringup_share) / 'preload'))
        self.declare_parameter(
            'subscriber_config_file', str(Path(bringup_share) / 'config' / 'subscribers.yaml')
        )
        self.declare_parameter(
            'preload_files',
            [''],
            descriptor=rclpy.node.ParameterDescriptor(type=rclpy.Parameter.Type.STRING_ARRAY),
        )

    def on_configure(self, state: LifecycleState) -> TransitionCallbackReturn:
        """Initialize the RDF store, preload files, and set up subscribers."""
        self.get_logger().info('Configuring KB node...')

        self.kb = TriplestarKBInterface(
            store_path=Path(self.get_parameter('store_path').value),
            logger=self.get_logger(),
        )
        self.get_logger().info(f'Using store path: {self.kb.store_path}')

        if not self._preload_files():
            self.get_logger().error('Failed to preload files')
            return TransitionCallbackReturn.ERROR

        subscriber_config_path = Path(self.get_parameter('subscriber_config_file').value)
        subscriber_config = yaml.safe_load(subscriber_config_path.read_text())

        self.subscriber_manager = SubscriberManager(self, config=subscriber_config)
        self.subscriber_manager.register_custom_functions(self.kb)

        self.get_logger().info('KB node configured successfully')
        return TransitionCallbackReturn.SUCCESS

    def on_cleanup(self, state: LifecycleState) -> TransitionCallbackReturn:
        """Clean up resources."""
        self.get_logger().info('Cleaning up KB node...')

        if self.kb:
            self.kb.close()
            self.kb = None

        self.subscriber_manager = None

        self.get_logger().info('KB node cleaned up')
        return TransitionCallbackReturn.SUCCESS

    def on_activate(self, state: LifecycleState) -> TransitionCallbackReturn:
        """Activate the KB node for serving."""
        self.get_logger().info('Activating KB node...')

        self.query_service = self.create_service(
            Query, f'{self.get_name()}/query', self.query_callback
        )

        result = super().on_activate(state)

        if result == TransitionCallbackReturn.SUCCESS:
            self.get_logger().info('KB node activated and ready to serve')
        else:
            self.get_logger().error('Failed to activate KB node')

        return result

    def on_deactivate(self, state: LifecycleState) -> TransitionCallbackReturn:
        """Deactivate the KB node but keep data in memory."""
        self.get_logger().info('Deactivating KB node...')

        result = super().on_deactivate(state)

        if result == TransitionCallbackReturn.SUCCESS:
            self.get_logger().info('KB node deactivated')
        else:
            self.get_logger().error('Failed to deactivate KB node')

        return result

    def on_shutdown(self, state: LifecycleState) -> TransitionCallbackReturn:
        """Final shutdown and cleanup."""
        self.get_logger().info('Shutting down KB node...')
        return TransitionCallbackReturn.SUCCESS

    def _preload_files(self) -> bool:
        """Preload TTL files from the configured preload directory."""
        if self.kb is None:
            self.get_logger().error('KB interface not initialized')
            return False

        preload_dir_value = self.get_parameter('preload_dir').value
        if not preload_dir_value:
            self.get_logger().warn('Preload dir parameter is empty, skipping preload')
            return True

        preload_dir = Path(preload_dir_value)

        if not preload_dir.exists() or not preload_dir.is_dir():
            self.get_logger().warn(
                f'Preload path {preload_dir} does not exist or is not a directory'
            )
            return False

        preload_files_value = self.get_parameter('preload_files').value
        if not preload_files_value:
            self.get_logger().warn('Preload files parameter is empty, skipping preload')
            return True

        file_paths = [
            preload_dir / name
            for name in preload_files_value
            if (preload_dir / name).is_file() and (preload_dir / name).suffix == '.ttl'
        ]

        if not file_paths:
            self.get_logger().warn(f'No valid .ttl files found in {preload_dir}')
            return False

        self.get_logger().info(f'Preloading {len(file_paths)} .ttl files from {preload_dir}')
        loaded_count = self.kb.load_files(file_paths)

        if loaded_count == 0:
            self.get_logger().warn(f'No files were successfully loaded from {preload_dir}')
            return False

        self.get_logger().info(f'Successfully preloaded {loaded_count} files from {preload_dir}')
        return True

    def query_callback(self, request: Query.Request, response: Query.Response) -> Query.Response:
        """Handle a SPARQL query request."""
        self.get_logger().debug(f'Received query request: {request}')

        if self.kb is None:
            self.get_logger().error('KB interface not initialized')
            response.success = False
            response.result = ''
            return response

        response.result = self.kb.query_json(request.query)
        response.success = response.result != ''

        return response
