from pathlib import Path
from threading import Lock

from jinja2 import Template
from jinja2 import TemplateNotFound
from rclpy.callback_groups import ReentrantCallbackGroup
from rclpy.clock import Clock
from rclpy.clock_type import ClockType
from rclpy.expand_topic_name import expand_topic_name
from rclpy.lifecycle import LifecycleNode
from rclpy.node import Node
from rosidl_runtime_py.utilities import get_service
from triplestar_core.config import InsertionServiceConfig
from triplestar_core.config import TriplestarConfig
from triplestar_core.insertion import make_insertion_environment
from triplestar_core.insertion_services.insertion_service import InsertionService
from triplestar_core.knowledge_base import KnowledgeBase
from triplestar_core.service_contract import insertion_mirror_name

_DISCOVERY_PERIOD_SEC = 0.2


class InsertionServiceManager:
    """Discover target service types and own their Triplestar mirrors."""

    def __init__(
        self,
        node: Node | LifecycleNode,
        config: TriplestarConfig,
        kb: KnowledgeBase,
        templates_dir: Path,
    ):
        self.node = node
        self.kb = kb
        self.logger = node.get_logger().get_child('insertion_services')
        self.callback_group = ReentrantCallbackGroup()
        self._steady_clock = Clock(clock_type=ClockType.STEADY_TIME)
        self.services: dict[str, InsertionService] = {}

        self._lock = Lock()
        self._running = False
        self._discovery_timer = None
        self._pending: set[str] = set()
        self._last_issue: dict[str, str] = {}

        env = make_insertion_environment(templates_dir)
        self._definitions: dict[str, tuple[InsertionServiceConfig, Template]] = {}
        for service_config in config.insertion_services:
            try:
                template = env.get_template(service_config.template)
            except TemplateNotFound as e:
                raise FileNotFoundError(
                    f'Insertion service template not found: {service_config.template}'
                ) from e

            target_name = expand_topic_name(
                service_config.service,
                node.get_name(),
                node.get_namespace(),
            )
            if target_name in self._definitions:
                raise ValueError(
                    f'Insertion service names resolve to the same target: {target_name}'
                )
            self._definitions[target_name] = (service_config, template)

    def start(self) -> None:
        """Start non-blocking service discovery."""
        with self._lock:
            if self._running:
                return
            self._running = True
            self._pending = set(self._definitions)
            self._discover_services_locked()
            if self._pending:
                self._discovery_timer = self.node.create_timer(
                    _DISCOVERY_PERIOD_SEC,
                    self._discover_services,
                    callback_group=self.callback_group,
                    clock=self._steady_clock,
                )
            mirror_names = sorted(self.services)
            pending_names = sorted(self._pending)

        self.logger.info(
            f'Started service ingestion; mirrors: {mirror_names}; '
            f'waiting for targets: {pending_names}'
        )

    def stop(self) -> None:
        """Stop discovery and destroy all clients and mirrors."""
        with self._lock:
            self._running = False
            self._destroy_discovery_timer_locked()
            for service in self.services.values():
                service.destroy()
            self.services.clear()
            self._pending.clear()
            self._last_issue.clear()
        self.logger.info('Stopped')

    def _discover_services(self) -> None:
        with self._lock:
            if self._running:
                self._discover_services_locked()

    def _discover_services_locked(self) -> None:
        advertised: dict[str, set[str]] = {}
        for name, service_types in self.node.get_service_names_and_types():
            advertised.setdefault(name, set()).update(service_types)

        for target_name in list(self._pending):
            type_names = sorted(advertised.get(target_name, set()))
            if not type_names:
                self._report_issue_once(
                    target_name,
                    f'Target service "{target_name}" is unavailable; discovery will retry',
                )
                continue
            if len(type_names) != 1:
                self._report_issue_once(
                    target_name,
                    f'Target service "{target_name}" advertises multiple types '
                    f'{type_names}; discovery will retry',
                )
                continue

            type_name = type_names[0]
            try:
                service_type = get_service(type_name)
            except (AttributeError, ImportError, ModuleNotFoundError, ValueError) as e:
                self._report_issue_once(
                    target_name,
                    f'Unable to load discovered type "{type_name}" for target service '
                    f'"{target_name}": {e}; discovery will retry',
                )
                continue

            service_config, template = self._definitions[target_name]
            try:
                mirror = InsertionService(
                    node=self.node,
                    target_name=target_name,
                    mirror_name=insertion_mirror_name(target_name),
                    service_type=service_type,
                    template=template,
                    update_fn=self.kb.update,
                    timeout_sec=service_config.timeout_sec,
                    callback_group=self.callback_group,
                    clock=self._steady_clock,
                )
            except Exception as e:  # noqa: BLE001
                self._report_issue_once(
                    target_name,
                    f'Failed to create mirror for "{target_name}": {e}; discovery will retry',
                )
                continue

            self.services[target_name] = mirror
            self._pending.remove(target_name)
            self._last_issue.pop(target_name, None)
            self.logger.info(f'Mirroring {target_name} ({type_name}) at {mirror.mirror_name}')

        if not self._pending:
            self._destroy_discovery_timer_locked()

    def _report_issue_once(self, target_name: str, message: str) -> None:
        if self._last_issue.get(target_name) == message:
            return
        self._last_issue[target_name] = message
        self.logger.warning(message)

    def _destroy_discovery_timer_locked(self) -> None:
        if self._discovery_timer is None:
            return
        self._discovery_timer.cancel()
        self.node.destroy_timer(self._discovery_timer)
        self._discovery_timer = None
