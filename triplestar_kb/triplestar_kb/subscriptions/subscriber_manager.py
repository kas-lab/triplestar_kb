import tf2_ros
from pyoxigraph import NamedNode

from triplestar_kb.kb_interface import TriplestarKBInterface
from triplestar_kb.msg_to_rdf import ros_msg_to_literal
from triplestar_kb.subscriptions.query_time_subscriber import (
    QueryTimeTFSubscriber,
    QueryTimeTopicSubscriber,
)

EX = 'http://example.org/'


class SubscriberManager:
    def __init__(self, node, config: dict):
        self.node = node

        # one shared tf buffer and listener for all TF lookups
        self._buffer = tf2_ros.Buffer()
        self._listener = tf2_ros.TransformListener(self._buffer, node)

        self.topic_query_subs: dict[str, QueryTimeTopicSubscriber] = {}
        self.tf_query_subs: dict[str, QueryTimeTFSubscriber] = {}

        self.logger = node.get_logger().get_child('subscriber_manager')

        self._load_config(config)

        self.logger.info(
            f'SubscriberManager initialized, available subscribers: '
            f'{list(self.topic_query_subs.keys()) + list(self.tf_query_subs.keys())}'
        )

    def _load_config(self, config: dict):
        for name, sub_cfg in config.get('query_time_topic_subscribers', {}).items():
            try:
                self.topic_query_subs[name] = QueryTimeTopicSubscriber(self.node, sub_cfg)
            except RuntimeError as e:
                self.logger.error(f'Failed to create topic subscriber "{name}": {e}')

        for name, sub_cfg in config.get('query_time_tf_subscribers', {}).items():
            self.tf_query_subs[name] = QueryTimeTFSubscriber(
                self.node, sub_cfg, self._buffer, self._listener
            )

        for name, sub_cfg in config.get('insertion_subscribers', {}).items():
            pass

    def register_custom_functions(self, kb: TriplestarKBInterface, base_uri: str = EX):
        all_subs = {**self.topic_query_subs, **self.tf_query_subs}
        for name, sub in all_subs.items():
            kb._add_custom_function(
                NamedNode(base_uri + name),
                lambda s=sub: ros_msg_to_literal(s.get_latest()),
            )
        self.logger.info(f'Registered {len(all_subs)} custom functions: {list(all_subs.keys())}')
