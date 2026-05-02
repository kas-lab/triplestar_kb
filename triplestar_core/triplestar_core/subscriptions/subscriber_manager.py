from pathlib import Path
from typing import Callable

import tf2_ros
from jinja2 import Environment, FileSystemLoader

from triplestar_core.knowledge_base import TriplestarKnowledgeBase
from triplestar_core.msg_to_rdf import ros_msg_to_literal
from triplestar_core.subscriptions.insertion_subscriber import InsertionSubscriber
from triplestar_core.subscriptions.query_time_subscriber import (
    TransformLatestSubscriber,
    TopicLatestSubscriber,
)


def _rdf_filter(value) -> str:
    literal = ros_msg_to_literal(value)
    return str(literal) if literal is not None else repr(value)


class SubscriptionManager:
    def __init__(self, node, config: dict, kb: TriplestarKnowledgeBase, templates_dir: Path):
        self.node = node
        self.logger = node.get_logger().get_child('subscriber_manager')

        self._buffer = tf2_ros.Buffer()
        self._listener = tf2_ros.TransformListener(self._buffer, node)

        self.topic_query_subs: dict[str, TopicLatestSubscriber] = {}
        self.tf_query_subs: dict[str, TransformLatestSubscriber] = {}
        self.insertion_subs: dict[str, InsertionSubscriber] = {}

        env = Environment(loader=FileSystemLoader(templates_dir))
        env.filters['rdf'] = _rdf_filter

        update_fn = self._make_update_fn(kb)

        self._load_topic_query_subs(config.get('query_time_topic_subscribers', {}))
        self._load_tf_query_subs(config.get('query_time_tf_subscribers', {}))
        self._load_insertion_subs(config.get('insertion_subscribers', {}), env, update_fn)

        # Register query-time subscribers as custom SPARQL functions
        all_query_subs = {**self.topic_query_subs, **self.tf_query_subs}

        for name, sub in all_query_subs.items():
            kb.add_query_time_function(
                name,
                lambda s=sub: ros_msg_to_literal(s.get_latest()),
            )

        self.logger.info(
            f'SubscriberManager initialized — '
            f'query-time: {list(all_query_subs.keys())}, '
            f'insertion: {list(self.insertion_subs.keys())}'
        )

    def _make_update_fn(self, kb: TriplestarKnowledgeBase) -> Callable[[str], None]:
        def update_fn(sparql: str) -> None:
            if kb.store is None:
                self.logger.error('KB store is not initialized, dropping insertion')
                return
            kb.store.update(sparql)

        return update_fn

    def _load_topic_query_subs(self, config: dict) -> None:
        for name, sub_cfg in config.items():
            try:
                topic = self._require(
                    sub_cfg, 'topic', context=f'query_time_topic_subscribers.{name}'
                )
                self.topic_query_subs[name] = TopicLatestSubscriber(
                    node=self.node,
                    topic=topic,
                    max_age_sec=sub_cfg.get('max_age_sec', 2.0),
                    msg_field_name=sub_cfg.get('msg_field_name'),
                )
            except (KeyError, RuntimeError) as e:
                self.logger.error(f'Failed to create topic query subscriber "{name}": {e}')

    def _load_tf_query_subs(self, config: dict) -> None:
        for name, sub_cfg in config.items():
            try:
                from_frame = self._require(
                    sub_cfg, 'from_frame', context=f'query_time_tf_subscribers.{name}'
                )
                to_frame = self._require(
                    sub_cfg, 'to_frame', context=f'query_time_tf_subscribers.{name}'
                )
                self.tf_query_subs[name] = TransformLatestSubscriber(
                    node=self.node,
                    from_frame=from_frame,
                    to_frame=to_frame,
                    buffer=self._buffer,
                    listener=self._listener,
                    max_age_sec=sub_cfg.get('max_age_sec', 2.0),
                )
            except (KeyError, RuntimeError) as e:
                self.logger.error(f'Failed to create TF query subscriber "{name}": {e}')

    def _load_insertion_subs(self, config: dict, env: Environment, update_fn: Callable) -> None:
        for name, sub_cfg in config.items():
            try:
                topic = self._require(sub_cfg, 'topic', context=f'insertion_subscribers.{name}')
                template_name = self._require(
                    sub_cfg, 'template', context=f'insertion_subscribers.{name}'
                )

                try:
                    template = env.get_template(template_name)
                except Exception as e:
                    raise RuntimeError(f'Unable to load template "{template_name}": {e}')

                self.insertion_subs[name] = InsertionSubscriber(
                    node=self.node,
                    topic=topic,
                    template=template,
                    update_fn=update_fn,
                )
            except (KeyError, RuntimeError) as e:
                self.logger.error(f'Failed to create insertion subscriber "{name}": {e}')

    @staticmethod
    def _require(cfg: dict, key: str, context: str) -> str:
        if key not in cfg or not cfg[key]:
            raise KeyError(f'Missing required field "{key}" in {context}')
        return cfg[key]
