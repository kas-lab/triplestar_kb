import importlib.util
import sys
import traceback
from pathlib import Path
from typing import Optional

import yaml
from ament_index_python import get_package_share_directory
from rclpy.lifecycle import LifecycleNode
from rclpy.lifecycle import LifecycleState
from rclpy.lifecycle import TransitionCallbackReturn
from triplestar_msgs.srv import SPARQLQuery

from triplestar_core.config import KBConfig
from triplestar_core.config import QueryServicesConfig
from triplestar_core.config import SubscribersConfig
from triplestar_core.functions import registry
from triplestar_core.knowledge_base import TriplestarKnowledgeBase
from triplestar_core.query_services.query_service_manager import QueryServiceManager
from triplestar_core.subscriptions.subscriber_manager import SubscriptionManager


class TriplestarKBNode(LifecycleNode):
    """
    A ROS2 lifecycle node for managing a triplestar knowledge base using pyoxigraph.
    """

    def __init__(self):
        super().__init__('triplestar_core')

        self.kb: Optional[TriplestarKnowledgeBase] = None
        self.subscriber_manager: Optional[SubscriptionManager] = None
        self.query_service_manager: Optional[QueryServiceManager] = None

        self.declare_parameter('bringup_package', 'triplestar_bringup')

        self.get_logger().info('Triplestar KB node created')

    def on_configure(self, state: LifecycleState) -> TransitionCallbackReturn:
        self.get_logger().info('Configuring KB node...')

        bringup_package = self.get_parameter('bringup_package').value

        try:
            share_dir = Path(get_package_share_directory(bringup_package))
        except Exception as e:
            self.get_logger().error(
                f'Could not find package share directory for "{bringup_package}": {e}'
            )
            return TransitionCallbackReturn.ERROR

        self.share_dir = share_dir  # cache it

        self.get_logger().info(f'Using config package: {bringup_package} ({share_dir})')

        try:
            # --- CONFIG LOAD ---
            self.config = self._load_kb_config(share_dir / 'config' / 'kb_params.yaml')

            # --- KB INIT ---
            self.kb = TriplestarKnowledgeBase(
                store_path=self.config.store_path,
                logger=self.get_logger(),
                base_iri=self.config.base_iri,
            )

            self.get_logger().info(f'Using store path: {self.kb.store_path}')

            # --- CLEAR ON STARTUP ---
            if self.config.clear_on_startup:
                self.kb.clear()
                self.get_logger().info('Cleared store on startup')

            # --- PRELOAD ---
            if not self._preload_files(share_dir / 'preload'):
                return TransitionCallbackReturn.ERROR

            # --- SUBSCRIBERS ---
            subscriber_config = self._load_subscribers_config(
                share_dir / 'config' / 'subscribers.yaml'
            )
            self.subscriber_manager = SubscriptionManager(
                self,
                config=subscriber_config,
                kb=self.kb,
                templates_dir=share_dir / 'templates',
            )

            # --- QUERY SERVICES ---
            query_config = self._load_query_service_config(
                share_dir / 'config' / 'query_services.yaml'
            )
            self.query_service_manager = QueryServiceManager(
                self,
                config=query_config,
                kb=self.kb,
                queries_dir=share_dir / 'queries',
            )

            # --- KB FUNCTIONS ---
            self._load_kb_functions(share_dir / 'functions')

            for name, func in registry:
                self.kb.add_kb_function(name, func)

        except Exception:
            self.get_logger().error(f'Configuration failed:\n{traceback.format_exc()}')
            return TransitionCallbackReturn.ERROR

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

    def _preload_files(self, preload_dir: Path) -> bool:
        """Preload TTL files from the given preload directory."""
        if self.kb is None:
            raise RuntimeError('KB interface not initialized')

        preload_files = self.config.preload_files

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

        loaded = self.kb.load_files(file_paths)

        if loaded == 0:
            self.get_logger().warn(f'No files were loaded from {preload_dir}')
            return False

        self.get_logger().info(f'Successfully preloaded {loaded} files from {preload_dir}')
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

    def _load_yaml(self, path: Path) -> dict:
        try:
            data = yaml.safe_load(path.read_text())
        except Exception as e:
            raise RuntimeError(f'Failed to load YAML: {path}') from e

        if data is None:
            return {}

        if not isinstance(data, dict):
            raise TypeError(f'Expected dict in YAML: {path}')

        return data

    def _load_kb_config(self, path: Path) -> KBConfig:
        data = self._load_yaml(path)
        return KBConfig.model_validate(data)

    def _load_subscribers_config(self, path: Path) -> SubscribersConfig:
        data = self._load_yaml(path)
        return SubscribersConfig.model_validate(data)

    def _load_query_service_config(self, path: Path) -> QueryServicesConfig:
        data = self._load_yaml(path)
        return QueryServicesConfig.model_validate(data)

    def _load_kb_functions(self, folder: Path):
        if not folder.is_dir():
            self.get_logger().warn(f'Functions directory {folder} does not exist')
            return

        for file in folder.glob('*.py'):
            module_name = file.stem
            spec = importlib.util.spec_from_file_location(module_name, file)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = module
                spec.loader.exec_module(module)
                self.get_logger().info(f'Loaded KB functions from {file}')
            else:
                self.get_logger().error(f'Failed to load KB functions from {file}')

        self.get_logger().info(f'KB functions loaded: {registry}')
