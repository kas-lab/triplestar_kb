import rclpy
from rclpy.executors import SingleThreadedExecutor
from triplestar_core.core_lifecycle_node import TriplestarCoreNode


def run(args=None):
    node = TriplestarCoreNode()
    # Mirrored service callbacks await ROS client futures cooperatively, so their
    # responses and timeout timers progress without concurrent KB access.
    executor = SingleThreadedExecutor()
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
