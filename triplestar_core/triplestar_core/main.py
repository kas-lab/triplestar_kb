import rclpy

from triplestar_core.core_lifecycle_node import TriplestarCoreNode


def run(args=None):
    node = TriplestarCoreNode()
    executor = rclpy.executors.SingleThreadedExecutor()
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
