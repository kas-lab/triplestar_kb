import rclpy
import rclpy.executors

from triplestar_core.kb_lifecycle_node import TriplestarKBNode


def run(args=None):
    node = TriplestarKBNode()
    executor = rclpy.executors.MultiThreadedExecutor()
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
