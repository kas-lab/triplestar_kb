from collections.abc import Callable

from jinja2 import Template
from opentelemetry import trace
from rclpy.callback_groups import ReentrantCallbackGroup
from rclpy.clock import Clock
from rclpy.lifecycle import LifecycleNode
from rclpy.node import Node
from triplestar_core.insertion import apply_insertion_template

TRACER = trace.get_tracer('triplestar_bench')


class InsertionService:
    """Forward one same-type service and insert facts from its response."""

    def __init__(
        self,
        node: Node | LifecycleNode,
        target_name: str,
        mirror_name: str,
        service_type: type,
        template: Template,
        update_fn: Callable[[str], None],
        timeout_sec: float,
        callback_group: ReentrantCallbackGroup,
        clock: Clock,
    ):
        self._node = node
        self._target_name = target_name
        self._mirror_name = mirror_name
        self._template = template
        self._update_fn = update_fn
        self._timeout_sec = timeout_sec
        self._callback_group = callback_group
        self._clock = clock
        self._logger = node.get_logger().get_child('InsertionService')

        self._client = node.create_client(
            service_type,
            target_name,
            callback_group=callback_group,
        )
        try:
            self._service = node.create_service(
                service_type,
                mirror_name,
                self._callback,
                callback_group=callback_group,
            )
        except Exception:
            node.destroy_client(self._client)
            raise

    @property
    def mirror_name(self) -> str:
        """Return the fully qualified mirror name."""
        return self._mirror_name

    @TRACER.start_as_current_span('insertion_service_callback')
    async def _callback(self, request, default_response):
        if not self._client.service_is_ready():
            self._logger.error(
                f'Target service "{self._target_name}" is unavailable; '
                'returning a default response'
            )
            return default_response

        future = self._client.call_async(request)

        def cancel_call() -> None:
            if not future.done():
                future.cancel()

        timer = self._node.create_timer(
            self._timeout_sec,
            cancel_call,
            callback_group=self._callback_group,
            clock=self._clock,
        )
        try:
            target_response = await future
        except Exception as e:  # noqa: BLE001
            self._logger.error(
                f'Call to target service "{self._target_name}" failed: {e}; '
                'returning a default response'
            )
            return default_response
        finally:
            self._node.destroy_timer(timer)

        if target_response is None:
            self._logger.error(
                f'Target service "{self._target_name}" timed out after '
                f'{self._timeout_sec:g}s; returning a default response'
            )
            return default_response

        try:
            apply_insertion_template(
                self._template,
                self._update_fn,
                request=request,
                response=target_response,
            )
        except Exception as e:  # noqa: BLE001
            self._logger.error(f'Insertion failed for {self._target_name}: {e}')

        return target_response

    def destroy(self) -> None:
        """Destroy the mirror and its target client."""
        self._node.destroy_service(self._service)
        self._node.destroy_client(self._client)
        self._logger.info(f'Mirror "{self._mirror_name}" destroyed')
