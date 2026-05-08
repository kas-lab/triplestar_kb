#!/usr/bin/env python3

import rclpy
import rclpy.executors

from triplestar_core.kb_lifecycle_node import TriplestarKBNode


def main(args=None):
    rclpy.init(args=args)
    node = TriplestarKBNode()

    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
