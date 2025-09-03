import random

import rclpy
from builtin_interfaces.msg import Time
from geometry_msgs.msg import Point32, Polygon
from rclpy.node import Node
from std_msgs.msg import Float32


class PolygonPublisher(Node):
    def __init__(self):
        super().__init__("polygon_publisher")

        self.polygon_publisher_ = self.create_publisher(Polygon, "/polygons", 10)
        self.time_publisher_ = self.create_publisher(Time, "/time", 10)
        self.battery_publisher_ = self.create_publisher(Float32, "/battery_level", 10)
        self.location_publisher_ = self.create_publisher(Point32, "/robot_pose", 10)

        self.timer = self.create_timer(1.0, self.publish_polygons)  # 1 Hz
        self.time_timer = self.create_timer(1.0, self.publish_time)  # 1 Hz
        self.battery_timer = self.create_timer(
            1.0, self.publish_battery_level
        )  # 0.2 Hz
        self.location_timer = self.create_timer(2.0, self.publish_location)

    def publish_polygons(self):
        polygon = Polygon()
        # Example polygon with 3 points (triangle)
        import random

        polygon.points = [
            Point32(x=random.uniform(0.0, 5.0), y=random.uniform(0.0, 5.0), z=0.0),
            Point32(x=random.uniform(0.0, 5.0), y=random.uniform(0.0, 5.0), z=0.0),
            Point32(x=random.uniform(0.0, 5.0), y=random.uniform(0.0, 5.0), z=0.0),
            Point32(x=random.uniform(0.0, 5.0), y=random.uniform(0.0, 5.0), z=0.0),
            Point32(x=random.uniform(0.0, 5.0), y=random.uniform(0.0, 5.0), z=0.0),
            Point32(
                x=random.uniform(0.0, 5.0), y=random.uniform(0.0, 5.0), z=0.0
            ),  # Closing the polygon
        ]
        self.polygon_publisher_.publish(polygon)

    def publish_time(self):
        now = self.get_clock().now().to_msg()
        self.time_publisher_.publish(now)

    def publish_battery_level(self):
        battery_level = Float32()
        battery_level.data = 75.0
        self.battery_publisher_.publish(battery_level)

    def publish_location(self):
        location = Point32()
        location.x = random.uniform(0.0, 10.0)
        location.y = random.uniform(0.0, 10.0)
        location.z = random.uniform(0.0, 10.0)
        self.location_publisher_.publish(location)


def main(args=None):
    rclpy.init(args=args)
    node = PolygonPublisher()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()
