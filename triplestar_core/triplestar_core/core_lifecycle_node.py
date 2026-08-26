import importlib.util
from pathlib import Path
import sys
import traceback

from ament_index_python import get_package_share_directory
from rclpy.lifecycle import LifecycleNode
from rclpy.lifecycle import LifecycleState
from rclpy.lifecycle import TransitionCallbackReturn
import yaml

from triplestar_core.config import KBConfig
from triplestar_core.config import QueryServicesConfig
from triplestar_core.config import SubscribersConfig
from triplestar_core.functions import registry
from triplestar_core.knowledge_base import KnowledgeBase
from triplestar_core.query_services.query_service_manager import QueryServiceManager
from triplestar_core.subscriptions.subscriber_manager import SubscriptionManager

# Exceptions we treat as "expected, fixable" configuration problems.
# Returning FAILURE for these keeps the node alive and retryable instead
# of falling through on_error's default -> Finalized.
_CONFIG_ERRORS = (FileNotFoundError, KeyError, ValueError, RuntimeError, TypeError)


class TriplestarCoreNode(LifecycleNode):
    """A ROS2 lifecycle node for managing a triplestar knowledge base using pyoxigraph."""

    def __init__(self):
        super().__init__('triplestar_core')

        self.kb: KnowledgeBase | None = None
        self.subscriber_manager: SubscriptionManager | None = None
        self.query_service_manager: QueryServiceManager | None = None
        self.query_service = None
        self.share_dir: Path | None = None
        self.config: KBConfig | None = None

        self.declare_parameter('bringup_package', 'triplestar_bringup')
        # allow setting this parameter to override with a single file to preload
        # (useful for tracing)
        self.declare_parameter('override_preload_file', value='')

        self.get_logger().info('Triplestar KB node created')

    # ------------------------------------------------------------------
    # Lifecycle callbacks
    # ------------------------------------------------------------------

    def on_configure(self, state: LifecycleState) -> TransitionCallbackReturn:
        """
        One-time, heavy setup: load config, init the store, clear + preload.

        Deliberately does NOT create live subscriptions or start the query
        service - that belongs to on_activate/on_deactivate so it can be
        redone cheaply (e.g. a subscribed topic disappears and comes back)
        without re-clearing or re-preloading the whole store.
        """
        self.get_logger().info('Configuring KB node...')

        try:
            self.share_dir = self._resolve_bringup_share_dir()
            self.config = self._load_kb_config(self.share_dir / 'config' / 'kb_params.yaml')

            self._init_knowledge_base()
            self._clear_and_preload(self.share_dir)

            self._build_subscription_manager(self.share_dir)
            self._build_query_service_manager(self.share_dir)
            self._load_kb_functions_into_kb(self.share_dir)

        except _CONFIG_ERRORS as e:
            self.get_logger().error(f'Configuration failed: {e}')
            return TransitionCallbackReturn.FAILURE
        except Exception:  # noqa: BLE001 follow best practices and log the exception
            self.get_logger().error(f'Unexpected error during configure:\n{traceback.format_exc()}')
            return TransitionCallbackReturn.ERROR

        self.get_logger().info('KB node configured successfully')
        return TransitionCallbackReturn.SUCCESS

    def on_activate(self, state: LifecycleState) -> TransitionCallbackReturn:
        """
        Bring the node fully online: subscribe to topics, serve queries.

        This is the retryable half of startup. If a subscribed topic isn't
        up yet, introspection/subscription creation fails here (not in
        configure), so a supervisor can just call activate again later
        without touching the preloaded data.
        """
        self.get_logger().info('Activating KB node...')

        try:
            if self.subscriber_manager is not None:
                self.subscriber_manager.start()
            if self.query_service_manager is not None:
                self.query_service_manager.start()
        except _CONFIG_ERRORS as e:
            self.get_logger().error(f'Activation failed: {e}')
            return TransitionCallbackReturn.FAILURE
        except Exception:  # noqa: BLE001
            self.get_logger().error(f'Unexpected error during activate:\n{traceback.format_exc()}')
            return TransitionCallbackReturn.ERROR

        result = super().on_activate(state)

        if result == TransitionCallbackReturn.SUCCESS:
            self.get_logger().warn('KB node activated and ready to serve')
        else:
            self.get_logger().error('Failed to activate KB node')

        return result

    def on_deactivate(self, state: LifecycleState) -> TransitionCallbackReturn:
        """Tear down subscriptions + query service, but keep data in memory."""
        self.get_logger().info('Deactivating KB node...')

        if self.subscriber_manager is not None:
            self.subscriber_manager.stop()  # destroys subscriptions, stops ingestion

        if self.query_service_manager is not None:
            self.query_service_manager.stop()

        result = super().on_deactivate(state)

        if result == TransitionCallbackReturn.SUCCESS:
            self.get_logger().warn('KB node deactivated')
        else:
            self.get_logger().error('Failed to deactivate KB node')

        return result

    def on_cleanup(self, state: LifecycleState) -> TransitionCallbackReturn:
        """Release everything on_configure set up."""
        self.get_logger().info('Cleaning up KB node...')

        if self.kb:
            self.kb.optimize()
            self.kb = None

        self.subscriber_manager = None
        self.query_service_manager = None
        self.config = None
        self.share_dir = None

        self.get_logger().info('KB node cleaned up')
        return TransitionCallbackReturn.SUCCESS

    def on_shutdown(self, state: LifecycleState) -> TransitionCallbackReturn:
        """Shut down and clean up the node."""
        self.get_logger().info('Shutting down KB node...')
        return TransitionCallbackReturn.SUCCESS

    # ------------------------------------------------------------------
    # on_configure helpers
    # ------------------------------------------------------------------

    def _resolve_bringup_share_dir(self) -> Path:
        bringup_package: str = self.get_parameter('bringup_package').value

        if not bringup_package:
            raise ValueError(
                'No bringup package specified. Please create a custom TriplestarKB '
                'bringup package and run from there (instructions in the README)'
            )

        try:
            share_dir = Path(get_package_share_directory(bringup_package))
        except (FileNotFoundError, KeyError, ValueError) as e:
            raise FileNotFoundError(
                f'Could not find package share directory for "{bringup_package}": {e}'
            ) from e

        self.get_logger().info(f'Using config package: {bringup_package} ({share_dir})')
        return share_dir

    def _init_knowledge_base(self):
        assert self.config is not None, 'No config loaded'

        self.kb = KnowledgeBase(
            store_path=self.config.store_path,
            logger=self.get_logger(),
            base_iri=self.config.base_iri,
        )
        self.get_logger().info(f'Using store path: {self.kb.store_path}')

    def _clear_and_preload(self, share_dir: Path):
        assert self.config is not None and self.kb is not None, (
            'No config loaded or KB not initialized'
        )

        if self.config.clear_on_startup:
            self.kb.clear()
            self.get_logger().info('Cleared store on startup')

        preload_dir = share_dir / 'preload'
        override_preload_file = self.get_parameter('override_preload_file').value
        preload_files = (
            [override_preload_file] if override_preload_file else self.config.preload_files
        )
        if override_preload_file:
            self.get_logger().warn(f'Overriding preload file: {override_preload_file}')

        self._preload_files(preload_dir, preload_files)

    def _preload_files(self, preload_dir: Path, preload_files: list[str]):
        """Preload TTL files from the given preload directory."""
        if self.kb is None:
            raise RuntimeError('KB interface not initialized')

        if not preload_files:
            self.get_logger().info('No preload files configured, skipping preload')
            return

        if not preload_dir.is_dir():
            raise FileNotFoundError(f'Preload directory {preload_dir} does not exist')

        file_paths = [
            preload_dir / name
            for name in preload_files
            if (preload_dir / name).is_file() and (preload_dir / name).suffix == '.ttl'
        ]

        if not file_paths:
            raise FileNotFoundError(f'No valid .ttl files found in {preload_dir}')

        loaded = self.kb.load_files(file_paths)

        if loaded == 0:
            raise RuntimeError(f'No files were loaded from {preload_dir}')

        self.get_logger().info(f'Successfully preloaded {loaded} files from {preload_dir}')
        self.get_logger().info(f'Amount of triples in the KB: {self.kb.count_triples()}')

    def _build_subscription_manager(self, share_dir: Path):
        """
        Construct the manager, but don't subscribe to anything yet.

        Actual topic introspection + subscription happens in on_activate,
        via subscriber_manager.start().
        """
        if self.kb is None:
            raise RuntimeError('KB not initialized')

        subscriber_config = self._load_subscribers_config(share_dir / 'config' / 'subscribers.yaml')
        self.subscriber_manager = (
            SubscriptionManager(
                self,
                config=subscriber_config,
                kb=self.kb,
                templates_dir=share_dir / 'templates',
            )
            if subscriber_config is not None
            else None
        )

    def _build_query_service_manager(self, share_dir: Path):
        """
        Build the query service manager from the query services configuration.

        Only start subscribing when start() is called
        """
        assert self.kb is not None
        query_config = self._load_query_service_config(share_dir / 'config' / 'query_services.yaml')
        self.query_service_manager = (
            QueryServiceManager(
                self,
                config=query_config,
                kb=self.kb,
                queries_dir=share_dir / 'queries',
            )
            if query_config is not None
            else None
        )

    def _load_kb_functions_into_kb(self, share_dir: Path):
        assert self.kb is not None
        self._load_kb_functions(share_dir / 'functions')
        for name, func in registry:
            self.kb.add_kb_function(name, func)

    # ------------------------------------------------------------------
    # Config file loading
    # ------------------------------------------------------------------

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
        return KBConfig.parse_obj(self._load_yaml(path))

    def _load_subscribers_config(self, path: Path) -> SubscribersConfig | None:
        if not path.is_file():
            self.get_logger().warn(
                f'Subscribers config file not found: {path}; skipping subscribers'
            )
            return None

        config = SubscribersConfig.parse_obj(self._load_yaml(path))

        if config.is_empty:
            self.get_logger().warn(
                f'Subscribers config file is empty: {path}; skipping subscribers'
            )
            return None

        return config

    def _load_query_service_config(self, path: Path) -> QueryServicesConfig | None:
        if not path.is_file():
            self.get_logger().warn(
                f'Query services config file not found: {path}; skipping query services'
            )
            return None

        config = QueryServicesConfig.parse_obj(self._load_yaml(path))

        if config.is_empty:
            self.get_logger().warn(
                f'Query services config file is empty: {path}; skipping query services'
            )
            return None

        return config

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
