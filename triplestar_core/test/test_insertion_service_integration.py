from pathlib import Path
from threading import Event
from threading import Thread
from time import sleep

import pytest
import rclpy
from rclpy.executors import SingleThreadedExecutor
from rclpy.node import Node
from std_srvs.srv import SetBool
from triplestar_core.config import TriplestarConfig
from triplestar_core.insertion_services.insertion_service_manager import InsertionServiceManager
from triplestar_core.knowledge_base import KnowledgeBase
from triplestar_core.service_contract import insertion_mirror_name


@pytest.fixture
def ros_context():
    rclpy.init()
    try:
        yield
    finally:
        rclpy.shutdown()


def test_mirror_forwards_response_and_updates_knowledge_graph(
    ros_context,
    tmp_path: Path,
):
    target_name = '/test_triplestar/set_enabled'
    mirror_name = insertion_mirror_name(target_name)
    template_path = tmp_path / 'set-enabled.sparql.tmpl'
    template_path.write_text(
        """
PREFIX ex: <https://example.org/>
INSERT DATA {
  ex:robot ex:enabled {{ response.success | rdf }} ;
           ex:message {{ response.message | rdf }} .
}
"""
    )

    target_node = Node('test_insertion_service_target')
    triplestar_node = Node('test_insertion_service_mirror')
    caller_node = Node('test_insertion_service_caller')
    # The Triplestar node and caller share the production single-threaded
    # executor. The target represents an independent ROS node/process.
    executor = SingleThreadedExecutor()
    target_executor = SingleThreadedExecutor()
    executor.add_node(triplestar_node)
    executor.add_node(caller_node)
    target_executor.add_node(target_node)

    def target_callback(request, response):
        if not request.data:
            sleep(0.5)
        response.success = request.data
        response.message = 'enabled' if request.data else 'disabled'
        return response

    kb = KnowledgeBase(store_path=None, logger=triplestar_node.get_logger())
    config = TriplestarConfig.parse_obj(
        {
            'knowledge_base': {
                'store_path': '',
                'base_iri': 'https://example.org/',
            },
            'insertion_services': [
                {
                    'service': target_name,
                    'template': template_path.name,
                    'timeout_sec': 0.2,
                }
            ],
        }
    )
    manager = InsertionServiceManager(triplestar_node, config, kb, tmp_path)
    manager.start()

    spin_thread = Thread(target=executor.spin, daemon=True)
    target_spin_thread = Thread(target=target_executor.spin, daemon=True)
    spin_thread.start()
    target_spin_thread.start()
    client = caller_node.create_client(SetBool, mirror_name)
    target_service = None
    try:
        assert not client.wait_for_service(timeout_sec=0.3)

        # The target comes online after activation. The manager must infer its
        # type from the graph and only then advertise the same-type mirror.
        target_service = target_node.create_service(SetBool, target_name, target_callback)
        assert client.wait_for_service(timeout_sec=5.0)

        done = Event()
        future = client.call_async(SetBool.Request(data=True))
        future.add_done_callback(lambda _: done.set())
        assert done.wait(timeout=5.0), 'mirror call did not complete'

        response = future.result()
        assert response.success is True
        assert response.message == 'enabled'
        assert (
            kb.query(
                """
PREFIX ex: <https://example.org/>
ASK { ex:robot ex:enabled true ; ex:message "enabled" . }
"""
            )
            is True
        )

        # A slow target must produce a default response without inserting its
        # late response or hanging the executor and caller.
        done = Event()
        future = client.call_async(SetBool.Request(data=False))
        future.add_done_callback(lambda _: done.set())
        assert done.wait(timeout=5.0), 'timed-out target call did not complete'
        assert future.result() == SetBool.Response()
        assert (
            kb.query(
                """
PREFIX ex: <https://example.org/>
ASK { ex:robot ex:message "disabled" . }
"""
            )
            is False
        )

        # ROS services have no transport-level error response. A target that
        # vanishes also finishes with a default response.
        sleep(0.5)
        target_node.destroy_service(target_service)
        target_service = None
        done = Event()
        future = client.call_async(SetBool.Request(data=True))
        future.add_done_callback(lambda _: done.set())
        assert done.wait(timeout=5.0), 'unavailable target call did not complete'
        assert future.result() == SetBool.Response()
    finally:
        manager.stop()
        caller_node.destroy_client(client)
        if target_service is not None:
            target_node.destroy_service(target_service)
        executor.shutdown(timeout_sec=5.0)
        target_executor.shutdown(timeout_sec=5.0)
        for node in (caller_node, triplestar_node):
            executor.remove_node(node)
            node.destroy_node()
        target_executor.remove_node(target_node)
        target_node.destroy_node()
        spin_thread.join(timeout=5.0)
        target_spin_thread.join(timeout=5.0)
