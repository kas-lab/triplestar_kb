import base64
from collections.abc import Callable
from pathlib import Path
import time

from jinja2 import Environment
from jinja2 import FileSystemLoader
from jinja2 import StrictUndefined
from jinja2 import TemplateNotFound
import rclpy
from rclpy.callback_groups import ReentrantCallbackGroup
from rclpy.lifecycle import LifecycleNode
from rclpy.node import Node
from rclpy.serialization import serialize_message
from ros2topic.api import get_msg_class
import tf2_ros

from triplestar_core.config import InsertionSubscriberConfig
from triplestar_core.config import QueryTimeTFSubscriberConfig
from triplestar_core.config import QueryTimeTopicSubscriberConfig
from triplestar_core.config import SubscribersConfig
from triplestar_core.conversions import to_rdf_literal
from triplestar_core.knowledge_base import KnowledgeBase
from triplestar_core.subscriptions.insertion_subscriber import InsertionSubscriber
from triplestar_core.subscriptions.query_time_subscriber import TopicLatestSubscriber
from triplestar_core.subscriptions.query_time_subscriber import TransformLatestSubscriber


def _serialize_filter(value) -> str:
    return base64.b64encode(serialize_message(value)).decode('utf-8')


def _rdf_filter(value) -> str:
    literal = to_rdf_literal(value)
    return str(literal) if literal is not None else repr(value)


def make_query_fn(sub):
    """Create a query function for the given subscriber."""
    return lambda: to_rdf_literal(sub.get_latest_msg())


class SubscriptionManager:
    """
    Owns all runtime subscriptions (query-time topic/TF + insertion).

    Construct once in on_configure (cheap - just stores config/refs).
    start()/stop() from on_activate/on_deactivate do the actual topic
    introspection and subscription creation/teardown, so it can be safely
    redone if a subscribed topic isn't up yet.
    """

    def __init__(
        self,
        node: Node | LifecycleNode,
        config: SubscribersConfig,
        kb: KnowledgeBase,
        templates_dir: Path,
    ):
        self.node = node
        self.config = config
        self.kb = kb
        self.templates_dir = templates_dir
        self.logger = node.get_logger().get_child('subscribers')

        self.subscriber_cb_group = ReentrantCallbackGroup()

        # Populated by start(), torn down by stop().
        self._buffer: tf2_ros.Buffer | None = None
        self._listener: tf2_ros.TransformListener | None = None
        self.topic_query_subs: dict[str, TopicLatestSubscriber] = {}
        self.tf_query_subs: dict[str, TransformLatestSubscriber] = {}
        self.insertion_subs: dict[str, InsertionSubscriber] = {}

    def start(self):
        self._buffer = tf2_ros.Buffer()
        self._listener = tf2_ros.TransformListener(self._buffer, self.node)

        env = Environment(
            loader=FileSystemLoader(self.templates_dir),
            autoescape=False,
            undefined=StrictUndefined,
        )
        env.filters['rdf'] = _rdf_filter
        env.filters['serialize'] = _serialize_filter

        self._load_topic_query_subs(self.config.query_time_topic_subscribers)
        self._load_tf_query_subs(self.config.query_time_tf_subscribers)
        self._load_insertion_subs(
            self.config.insertion_subscribers,
            env,
            lambda sparql: self.kb.update(sparql),
        )

        # Register query-time subscribers as custom SPARQL functions
        all_query_subs = {**self.topic_query_subs, **self.tf_query_subs}
        for name, sub in all_query_subs.items():
            self.kb.add_query_time_function(name, make_query_fn(sub))

        self.logger.info(
            f'Started — query-time: {list(all_query_subs.keys())}, '
            f'insertion: {list(self.insertion_subs.keys())}'
        )

    def stop(self):
        # Unregister SPARQL functions before tearing down what backs them.
        for name in {**self.topic_query_subs, **self.tf_query_subs}:
            self.kb.remove_query_time_function(name)

        for sub in self.insertion_subs.values():
            sub.destroy()
        self.insertion_subs.clear()

        for sub in self.topic_query_subs.values():
            sub.destroy()
        self.topic_query_subs.clear()

        if self._listener is not None:
            self._listener.unregister()
        self._buffer = None
        self._listener = None

        self.logger.info('Stopped')

    def try_msg_class(self, topic: str, timeout_sec: float = 2.0) -> type | None:
        start = time.time()
        self.logger.info(f"Waiting for message class for topic '{topic}'...")

        while rclpy.ok():
            if topic in [t[0] for t in self.node.get_topic_names_and_types()]:
                break

            if time.time() - start > timeout_sec:
                self.logger.warning(f"Timeout waiting for message class for topic '{topic}'")
                return None

            time.sleep(0.2)

        msg_type = get_msg_class(self.node, topic, include_hidden_topics=True)
        return msg_type if msg_type else None

    def _load_topic_query_subs(self, config: dict[str, QueryTimeTopicSubscriberConfig]) -> None:
        for name, sub in config.items():
            msg_type = self.try_msg_class(sub.topic)
            if msg_type is None:
                self.logger.error(f'Unable to determine message class for topic: {sub.topic}')
                continue

            try:
                self.topic_query_subs[name] = TopicLatestSubscriber(
                    node=self.node,
                    logger=self.logger,
                    topic=sub.topic,
                    msg_type=msg_type,
                    msg_field_name=sub.msg_field_name,
                    callback_group=self.subscriber_cb_group,
                )
            except (KeyError, RuntimeError) as e:
                self.logger.error(f'Failed to create topic query subscriber "{name}": {e}')

    def _load_tf_query_subs(
        self,
        config: dict[str, QueryTimeTFSubscriberConfig],
    ) -> None:
        assert self._buffer is not None, (
            'buffer must be initialized before loading TF query subscribers'
        )
        assert self._listener is not None, (
            'listener must be initialized before loading TF query subscribers'
        )

        for name, sub in config.items():
            try:
                self.tf_query_subs[name] = TransformLatestSubscriber(
                    node=self.node,
                    logger=self.logger,
                    from_frame=sub.from_frame,
                    to_frame=sub.to_frame,
                    buffer=self._buffer,
                    listener=self._listener,
                )
            except (KeyError, RuntimeError) as e:
                self.logger.error(f'Failed to create TF query subscriber "{name}": {e}')

    def _load_insertion_subs(
        self,
        config: dict[str, InsertionSubscriberConfig],
        env: Environment,
        update_fn: Callable,
    ) -> None:
        for name, sub in config.items():
            try:
                template = env.get_template(sub.template)
            except TemplateNotFound as e:
                self.logger.error(f'Unable to load template "{sub.template}": {e}')
                continue

            msg_type = self.try_msg_class(sub.topic)
            if msg_type is None:
                self.logger.error(f'Unable to determine message class for topic: {sub.topic}')
                continue

            try:
                self.insertion_subs[name] = InsertionSubscriber(
                    node=self.node,
                    topic=sub.topic,
                    template=template,
                    update_fn=update_fn,
                    msg_type=msg_type,
                    callback_group=self.subscriber_cb_group,
                )
            except Exception as e:  # noqa: BLE001
                self.logger.error(f'Failed to create insertion subscriber "{name}": {e}')
