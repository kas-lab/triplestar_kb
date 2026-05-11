from jinja2 import Template
from rclpy.lifecycle import LifecycleNode
from rclpy.node import Node


class InsertionSubscriber:
    def __init__(
        self,
        node: Node | LifecycleNode,
        topic: str,
        template: Template,
        update_fn,
        msg_type,
    ):
        self._node = node
        self._logger = node.get_logger().get_child('InsertionSubscriber')

        self._template = template
        self._update_fn = update_fn
        self._topic = topic

        self._sub = node.create_subscription(
            msg_type,
            self._topic,
            self._callback,
            10,
        )

        self._logger.info(f'Subscribed to {self._topic}')

    def _callback(self, msg):
        try:
            query = self._template.render(msg=msg)
            self._update_fn(query)
        except Exception as e:
            self._logger.error(f'Insertion failed for {self._topic}: {e}')
