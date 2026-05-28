import time
from pathlib import Path
from typing import Callable
from typing import Optional
from typing import Type

import rclpy
import tf2_ros
from jinja2 import Environment
from jinja2 import FileSystemLoader
from jinja2 import StrictUndefined
from jinja2 import TemplateNotFound
from rclpy.callback_groups import ReentrantCallbackGroup
from rclpy.lifecycle import LifecycleNode
from rclpy.node import Node
from ros2topic.api import get_msg_class

from triplestar_core.config import InsertionSubscriberConfig
from triplestar_core.config import QueryTimeTFSubscriberConfig
from triplestar_core.config import QueryTimeTopicSubscriberConfig
from triplestar_core.config import SubscribersConfig
from triplestar_core.knowledge_base import TriplestarKnowledgeBase
from triplestar_core.msg_to_rdf import ros_msg_to_literal
from triplestar_core.subscriptions.insertion_subscriber import InsertionSubscriber
from triplestar_core.subscriptions.query_time_subscriber import TopicLatestSubscriber
from triplestar_core.subscriptions.query_time_subscriber import TransformLatestSubscriber


def _rdf_filter(value) -> str:
    literal = ros_msg_to_literal(value)
    return str(literal) if literal is not None else repr(value)


# Returns a function that queries the latest message from the subscriber and converts it to an RDF literal, which can be registered in the KB.
def make_query_fn(sub):
    return lambda: ros_msg_to_literal(sub.get_latest_msg())


class SubscriptionManager:
    def __init__(
        self,
        node: Node | LifecycleNode,
        config: SubscribersConfig,
        kb: TriplestarKnowledgeBase,
        templates_dir: Path,
    ):
        self.node = node
        self.logger = node.get_logger().get_child('subscriber_manager')

        self.subscriber_cb_group = ReentrantCallbackGroup()

        self._buffer = tf2_ros.Buffer()
        self._listener = tf2_ros.TransformListener(self._buffer, node)

        self.topic_query_subs: dict[str, TopicLatestSubscriber] = {}
        self.tf_query_subs: dict[str, TransformLatestSubscriber] = {}
        self.insertion_subs: dict[str, InsertionSubscriber] = {}

        env = Environment(
            loader=FileSystemLoader(templates_dir),
            autoescape=False,
            undefined=StrictUndefined,
        )
        env.filters['rdf'] = _rdf_filter

        self._load_topic_query_subs(
            config.query_time_topic_subscribers,
        )
        self._load_tf_query_subs(config.query_time_tf_subscribers)
        self._load_insertion_subs(
            config.insertion_subscribers,
            env,
            lambda sparql: kb.update(sparql),
        )

        # Register query-time subscribers as custom SPARQL functions
        all_query_subs = {**self.topic_query_subs, **self.tf_query_subs}

        for name, sub in all_query_subs.items():
            kb.add_query_time_function(name, make_query_fn(sub))

        self.logger.info(
            f'SubscriberManager initialized — '
            f'query-time: {list(all_query_subs.keys())}, '
            f'insertion: {list(self.insertion_subs.keys())}'
        )

    def try_msg_class(self, topic: str, timeout_sec: float = 2.0) -> Optional[Type]:
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
        for name, sub in config.items():
            try:
                self.tf_query_subs[name] = TransformLatestSubscriber(
                    node=self.node,
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
            except Exception as e:
                self.logger.error(f'Failed to create insertion subscriber "{name}": {e}')
