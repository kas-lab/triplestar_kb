import time
from typing import Any, Optional

import rclpy
import tf2_ros
from rclpy.lifecycle import LifecycleNode
from rclpy.node import Node
from rclpy.time import Time
from ros2topic.api import get_msg_class


class BaseQueryTimeSubscriber:
    def __init__(
        self,
        node: Node | LifecycleNode,
        max_age_sec: float = 2.0,
    ) -> None:
        self._node = node
        self._max_age_sec = max_age_sec
        self._logger = node.get_logger().get_child(self.__class__.__name__)

    def get_latest(self, *args, **kwargs) -> Optional[Any]:
        """return the latest value if available and fresh, else None."""
        raise NotImplementedError('get_latest must be implemented by subclasses')


class QueryTimeTopicSubscriber(BaseQueryTimeSubscriber):
    def __init__(self, node: Node | LifecycleNode, config: dict):
        super().__init__(node, config.get('max_age_sec', 2.0))
        self._topic_name = config.get('topic_name', '')
        self._msg_field_name = config.get('msg_field_name', None)
        self._latest_msg = None
        self._latest_time = None

        if not self.wait_for_topic():
            raise RuntimeError(f'Topic {self._topic_name} not available')

        msg_type = get_msg_class(node, self._topic_name)

        if msg_type is None:
            raise RuntimeError(f'Unable to determine message class for topic: {self._topic_name}')

        if self._msg_field_name is not None:
            if not hasattr(msg_type, self._msg_field_name):
                raise RuntimeError(
                    f'Message type {msg_type} does not have field {self._msg_field_name}'
                )
            else:
                self._logger.info(
                    f'Subscribing to topic: {self._topic_name} with message type: {msg_type} and field: {self._msg_field_name}'
                )

        self._subscription = self._node.create_subscription(
            msg_type,
            self._topic_name,
            self._callback,
            10,
        )

        self._logger.info(f'Subscribed to topic: {self._topic_name}')

    def wait_for_topic(
        self,
        timeout_sec: float = 2.0,
        poll_interval: float = 2,
    ) -> bool:
        start_time = time.time()
        while rclpy.ok():
            topics = [t[0] for t in self._node.get_topic_names_and_types()]
            if self._topic_name in topics:
                self._logger.info(f'Topic {self._topic_name} is now available.')
                return True
            if time.time() - start_time > timeout_sec:
                self._logger.error(f'Timeout waiting for topic {self._topic_name}.')
                return False
            self._logger.info(f'Waiting for topic {self._topic_name} to become available...')
            time.sleep(poll_interval)
        return False

    def _callback(self, msg):
        self._latest_msg = msg
        self._logger.debug(f'feedback received: {self._latest_msg}')
        # Use header timestamp if available, else wall time
        if hasattr(msg, 'header') and hasattr(msg.header, 'stamp'):
            self._latest_time = msg.header.stamp.sec + msg.header.stamp.nanosec * 1e-9
        else:
            self._latest_time = time.time()

    def get_latest(self, *args, **kwargs):
        self._logger.debug('get_latest called')
        if not self._latest_msg or not self._latest_time:
            return None

        if (time.time() - self._latest_time) < self._max_age_sec:
            return (
                getattr(self._latest_msg, self._msg_field_name, self._latest_msg)
                if self._msg_field_name
                else self._latest_msg
            )

        return None


class QueryTimeTFSubscriber(BaseQueryTimeSubscriber):
    def __init__(
        self,
        node,
        config: dict,
        buffer: tf2_ros.Buffer,
        listener: tf2_ros.TransformListener,
    ):
        super().__init__(node, config.get('max_age_sec', 2.0))

        self.config = config
        self._buffer = buffer
        self._listener = listener

    def get_latest(self):
        self._logger.debug(
            f'get_latest called for TF: {self.config["from_frame"]} -> {self.config["to_frame"]}'
        )
        try:
            transform = self._buffer.lookup_transform(
                self.config['to_frame'],
                self.config['from_frame'],
                Time(),
                timeout=rclpy.duration.Duration(seconds=1.0),  # type: ignore
            )
            self._logger.debug(f'TF transform received: {transform}')
            return transform.transform.translation

        except Exception as e:
            self._logger.warn(
                f'TF lookup failed for {self.config["from_frame"]} -> {self.config["to_frame"]}: {e}'
            )
            return None
