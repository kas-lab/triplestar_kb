import rclpy
from rclpy.executors import MultiThreadedExecutor
from triplestar_core.core_lifecycle_node import TriplestarCoreNode


def run(args=None):
    node = TriplestarCoreNode()
    # Mirrored service callbacks asynchronously await target clients. Multiple
    # executor threads let those client responses and timeout timers make progress.
    executor = MultiThreadedExecutor(num_threads=4)
    executor.add_node(node)

    try:
        node.get_logger().info('Starting TriplestarKBNode')
        executor.spin()
    finally:
        node.destroy_node()


def main(args=None):
    rclpy.init(args=args)

    try:
        run(args)
    except KeyboardInterrupt:
        pass
    finally:
        rclpy.try_shutdown()


if __name__ == '__main__':
    main()
