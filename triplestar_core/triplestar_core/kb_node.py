#!/usr/bin/env python3

import rclpy
import rclpy.executors

from triplestar_core.kb_lifecycle_node import TriplestarKBNode


def main(args=None):
    rclpy.init(args=args)
    node = TriplestarKBNode()
    executor = rclpy.executors.MultiThreadedExecutor()
    executor.add_node(node)

    try:
        node.get_logger().info('Starting TriplestarKBNode')
        executor.spin()
    except KeyboardInterrupt:
        node.get_logger().info('Keyboard interrupt, shutting down.\n')

    node.destroy_node()
    rclpy.shutdown()


if __name__ == '__main__':
    main()
