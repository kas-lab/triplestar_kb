from jinja2 import Template
from rclpy.lifecycle import LifecycleNode
from rclpy.node import Node
from ros2topic.api import get_msg_class

from triplestar_core.subscriptions.query_time_subscriber import wait_for_topic


class InsertionSubscriber:
    def __init__(
        self,
        node: Node | LifecycleNode,
        topic: str,
        template: Template,
        update_fn,
    ):
        self._node = node
        self._logger = node.get_logger().get_child('InsertionSubscriber')
        self._topic = topic
        self._update_fn = update_fn
        self._template = template

        if not wait_for_topic(node, self._topic):
            raise RuntimeError(f'Topic {self._topic} not available')

        msg_type = get_msg_class(node, self._topic)
        if msg_type is None:
            raise RuntimeError(f'Unable to determine message class for {self._topic}')

        self._sub = node.create_subscription(msg_type, self._topic, self._callback, 10)
        self._logger.info(f'Subscribed to {self._topic}')

    def _callback(self, msg):
        try:
            query = self._template.render(msg=msg)
            self._update_fn(query)
            self._logger.debug(f'Insertion succeeded: {query[:100]}...')
        except Exception as e:
            self._logger.error(f'Insertion failed for {self._topic}: {e}')
