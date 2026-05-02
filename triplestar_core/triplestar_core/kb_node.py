#!/usr/bin/env python3

import rclpy
import rclpy.executors

from triplestar_core.ros_kb_interface import TriplestarKBNode


def main(args=None):
    rclpy.init(args=args)
    node = TriplestarKBNode()

    executor = rclpy.executors.MultiThreadedExecutor()
    executor.add_node(node)
    try:
        executor.spin()
    except (KeyboardInterrupt, rclpy.executors.ExternalShutdownException):
        pass
    finally:
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == '__main__':
    main()
