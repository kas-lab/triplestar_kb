import time
from typing import Any, Optional, Type

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

    def get_latest(self) -> Optional[Any]:
        """return the latest value if available and fresh, else None."""
        raise NotImplementedError("get_latest must be implemented by subclasses")


class QueryTimeSubscriber(BaseQueryTimeSubscriber):
    def __init__(
        self,
        node: Node | LifecycleNode,
        topic_name: str,
        msg_field_name: Optional[str] = None,
        msg_type: Optional[Type] = None,
        max_age_sec=2.0,
    ):
        super().__init__(node, max_age_sec)
        self._latest_msg: Optional[Any] = None
        self._latest_time: Optional[float] = None
        self._topic_name = topic_name
        self._msg_field_name = msg_field_name

        self.wait_for_topic()
        msg_type = (
            get_msg_class(node, self._topic_name) if msg_type is None else msg_type
        )
        if msg_type is None:
            self._logger.error(
                f"Unable to determine message class for topic: {self._topic_name}"
            )
            raise RuntimeError(
                f"Message type could not be determined for topic {self._topic_name}"
            )

        if self._msg_field_name is not None:
            if not hasattr(msg_type, self._msg_field_name):
                self._logger.error(
                    f"Message type {msg_type} does not have field {self._msg_field_name}"
                )
            else:
                self._logger.info(
                    f"Subscribing to topic: {self._topic_name} with message type: {msg_type} and field: {self._msg_field_name}"
                )

        node.create_subscription(msg_type, self._topic_name, self._callback, 10)
        self._logger.info(f"Subscribed to topic: {self._topic_name}")

    def wait_for_topic(
        self,
        timeout_sec: float = 30.0,
        poll_interval: float = 2,
    ) -> bool:
        start_time = time.time()
        while rclpy.ok():
            topics = [t[0] for t in self._node.get_topic_names_and_types()]
            if self._topic_name in topics:
                self._logger.info(f"Topic {self._topic_name} is now available.")
                return True
            if time.time() - start_time > timeout_sec:
                self._logger.error(f"Timeout waiting for topic {self._topic_name}.")
                return False
            self._logger.info(
                f"Waiting for topic {self._topic_name} to become available..."
            )
            time.sleep(poll_interval)
        return False

    def _callback(self, msg):
        self._latest_msg = msg
        self._logger.debug(f"feedback received: {self._latest_msg}")
        # Use header timestamp if available, else wall time
        if hasattr(msg, "header") and hasattr(msg.header, "stamp"):
            self._latest_time = msg.header.stamp.sec + msg.header.stamp.nanosec * 1e-9
        else:
            self._latest_time = time.time()

    def get_latest(self):
        self._logger.debug("get_latest called")
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
        node: Node | LifecycleNode,
        from_frame: str,
        to_frame: str,
        max_age_sec: float = 2.0,
    ):
        super().__init__(node, max_age_sec)
        self._from_frame = from_frame
        self._to_frame = to_frame

        self._buffer = tf2_ros.Buffer()
        self._listener = tf2_ros.TransformListener(self._buffer, node)
        self._latest_time: Optional[float] = None

        self._logger.info(
            f"Initialized TF subscriber:{self._from_frame} -> {self._to_frame}"
        )

    def get_latest(self):
        self._logger.info("get_latest called for TF")
        try:
            transform = self._buffer.lookup_transform(
                self._to_frame,
                self._from_frame,
                Time(),
                timeout=rclpy.duration.Duration(seconds=1.0),  # type: ignore
            )
            self._logger.info(f"TF transform received: {transform}")
            return transform.transform.translation

        except Exception as e:
            self._logger.warn(f"TF lookup failed: {e}")
            return None
