from triplestar_kb.kb_interface import TriplestarKBInterface
from triplestar_kb.query_services.query_service import QueryService


class QueryServiceManager:
    def __init__(self, node, config: dict, kb: TriplestarKBInterface):
        self.node = node
        self.logger = node.get_logger().get_child('query_service_manager')

        self.query_services: dict[str, QueryService] = {}

        for name, sub_cfg in config.items():
            try:
                self.query_services[name] = QueryService(sub_cfg, kb)
            except RuntimeError as e:
                self.logger.error(f'Failed to create topic subscriber "{name}": {e}')
        self._load_config(config, kb)

        self.logger.info(
            f'QueryServiceManager initialized — query-services: {list(self.query_services.keys())}'
        )

    def _load_config(self, config: dict, kb: TriplestarKBInterface):

        def query_fn(sparql: str) -> None:
            if kb.store is None:
                self.logger.error('KB store is not initialized, dropping query')
                return
            kb.query_json(sparql)

        # for name, sub_cfg in config.get('insertion_subscribers', {}).items():
        #     try:
        #         self.insertion_subs[name] = InsertionSubscriber(self.node, sub_cfg, env, update_fn)
        #     except RuntimeError as e:
        #         self.logger.error(f'Failed to create insertion subscriber "{name}": {e}')
