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
from triplestar_msgs.srv import SPARQLQuery

from triplestar_core.kb_interface import TriplestarKBInterface
from triplestar_core.query_services.query_service_manager import QueryServiceManager
from triplestar_core.subscriptions.subscriber_manager import SubscriberManager


class RosTriplestarKBInterface(LifecycleNode):
    """
    A ROS2 lifecycle node for managing a triplestar knowledge base using pyoxigraph.
    """

    def __init__(self):
        super().__init__('triplestar_core')

        self.kb: Optional[TriplestarKBInterface] = None
        self.subscriber_manager: Optional[SubscriberManager] = None
        self.query_service_manager: Optional[QueryServiceManager] = None

        self._declare_parameters()

        self.get_logger().info('Triplestar KB node created')

    def _declare_parameters(self):
        """Declare all node parameters with their default values."""
        self.declare_parameter('store_path', '/tmp/triplestar_core')
        self.declare_parameter('config_package', 'triplestar_bringup')
        self.declare_parameter(
            'preload_files',
            [''],
            descriptor=rclpy.node.ParameterDescriptor(type=rclpy.Parameter.Type.STRING_ARRAY),
        )
        self.declare_parameter('base_iri', 'http://triplestar.local')

    def on_configure(self, state: LifecycleState) -> TransitionCallbackReturn:
        """Initialize the RDF store, preload files, and set up subscribers and query services."""
        self.get_logger().info('Configuring KB node...')

        self.kb = TriplestarKBInterface(
            store_path=Path(self.get_parameter('store_path').value),
            logger=self.get_logger(),
            base_iri=self.get_parameter('base_iri').value,
        )
        self.get_logger().info(f'Using store path: {self.kb.store_path}')

        config_pkg = self.get_parameter('config_package').value
        try:
            share_dir = Path(get_package_share_directory(config_pkg))
        except Exception as e:
            self.get_logger().error(
                f'Could not find package share directory for "{config_pkg}": {e}'
            )
            return TransitionCallbackReturn.ERROR

        self.get_logger().info(f'Using config package: {config_pkg} ({share_dir})')

        if not self._preload_files(share_dir / 'preload'):
            self.get_logger().error('Failed to preload files')
            return TransitionCallbackReturn.ERROR

        subscriber_config = self._load_yaml_config(share_dir / 'config' / 'subscribers.yaml')
        if subscriber_config is None:
            return TransitionCallbackReturn.ERROR
        self.subscriber_manager = SubscriberManager(
            self, config=subscriber_config, kb=self.kb, templates_dir=share_dir / 'templates'
        )

        query_services_config = self._load_yaml_config(share_dir / 'config' / 'query_services.yaml')
        if query_services_config is None:
            return TransitionCallbackReturn.ERROR
        self.query_service_manager = QueryServiceManager(
            self, config=query_services_config, kb=self.kb, queries_dir=share_dir / 'queries'
        )

        self.get_logger().info('KB node configured successfully')
        return TransitionCallbackReturn.SUCCESS

    def on_cleanup(self, state: LifecycleState) -> TransitionCallbackReturn:
        """Clean up resources."""
        self.get_logger().info('Cleaning up KB node...')

        if self.kb:
            self.kb.optimize()
            self.kb = None

        self.subscriber_manager = None
        self.query_service_manager = None

        self.get_logger().info('KB node cleaned up')
        return TransitionCallbackReturn.SUCCESS

    def on_activate(self, state: LifecycleState) -> TransitionCallbackReturn:
        """Activate the KB node for serving."""
        self.get_logger().info('Activating KB node...')

        self.query_service = self.create_service(
            SPARQLQuery, f'{self.get_name()}/query', self.query_callback
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

        self.query_service = None

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

    def _load_yaml_config(self, config_path: Path) -> Optional[dict]:
        """Load a YAML config file from the given path."""
        try:
            return yaml.safe_load(config_path.read_text()) or {}
        except FileNotFoundError:
            self.get_logger().error(f'Config file not found: {config_path}')
            return None
        except yaml.YAMLError as e:
            self.get_logger().error(f'Failed to parse config file {config_path}: {e}')
            return None

    def _preload_files(self, preload_dir: Path) -> bool:
        """Preload TTL files from the given preload directory."""
        if self.kb is None:
            self.get_logger().error('KB interface not initialized')
            return False

        # Strip the [''] placeholder ROS2 uses as the default for string array parameters.
        preload_files = [f for f in self.get_parameter('preload_files').value if f]
        if not preload_files:
            self.get_logger().info('No preload files configured, skipping preload')
            return True

        if not preload_dir.is_dir():
            self.get_logger().warn(f'Preload directory {preload_dir} does not exist')
            return False

        file_paths = [
            preload_dir / name
            for name in preload_files
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
        self.get_logger().info(f'Amount of triples in the KB: {self.kb.count_triples()}')
        return True

    def query_callback(
        self, request: SPARQLQuery.Request, response: SPARQLQuery.Response
    ) -> SPARQLQuery.Response:
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
