import json

import rclpy
from geometry_msgs.msg import Point
from rclpy.node import Node
from shapely import wkt
from triplestar_kb_msgs.srv import Query
from visualization_msgs.msg import Marker, MarkerArray

EX = "http://example.org/"
GEO = "http://www.opengis.net/ont/geosparql#"


class KBPolygonViz(Node):
    def __init__(self):
        super().__init__("kb_polygon_viz")

        self.publisher = self.create_publisher(MarkerArray, "kb_markers", 10)
        self.timer = self.create_timer(
            1.0, self.update_markers_periodically
        )  # 1 Hz update

        self.get_logger().info("KB Polygon Visualizer node started")

    def send_sparql_query_to_kb(self, sparql: str):
        client = self.create_client(Query, "/triplestar_kb/query")
        while not client.wait_for_service(timeout_sec=1.0):
            self.get_logger().warn("Waiting for /triplestar_kb/query service...")

        request = Query.Request()
        request.query_type = 1
        request.query = sparql

        self.get_logger().debug(f"Sending SPARQL query:\n{sparql}")

        future = client.call_async(request)
        future.add_done_callback(self.process_query_results)

    def process_query_results(self, future):
        try:
            response = future.result()
            result_json = response.result
            results = json.loads(result_json)["results"]["bindings"]

            marker_array = MarkerArray()

            for idx, row in enumerate(results):
                poly = wkt.loads(row["wkt"]["value"])

                # Convert polygon coordinates to geometry_msgs/Point
                points = [Point(x=pt[0], y=pt[1], z=0.0) for pt in poly.exterior.coords]

                # Generate a deterministic color based on the room URI
                room_uri = row["room"]["value"]
                color_hash = hash(room_uri)
                r = ((color_hash >> 16) & 0xFF) / 255.0
                g = ((color_hash >> 8) & 0xFF) / 255.0
                b = (color_hash & 0xFF) / 255.0

                marker = Marker()
                marker.header.frame_id = "map"
                marker.header.stamp = self.get_clock().now().to_msg()
                marker.ns = "kb_rooms"
                marker.id = idx
                marker.type = Marker.LINE_STRIP
                marker.action = Marker.ADD
                marker.scale.x = 0.05  # line thickness
                marker.color.r = r
                marker.color.g = g
                marker.color.b = b
                marker.color.a = 1.0
                marker.points = points
                marker.pose.orientation.w = 1.0  # default pose

                marker_array.markers.append(marker)

                # Add a text marker for the label or URI
                text_marker = Marker()
                text_marker.header.frame_id = "map"
                text_marker.header.stamp = self.get_clock().now().to_msg()
                text_marker.ns = "kb_rooms_labels"
                text_marker.id = idx + len(results)  # Ensure unique ID
                text_marker.type = Marker.TEXT_VIEW_FACING
                text_marker.action = Marker.ADD
                text_marker.scale.z = 0.5  # text size
                text_marker.color.r = r
                text_marker.color.g = g
                text_marker.color.b = b
                text_marker.color.a = 1.0
                text_marker.pose.position.x = poly.centroid.x
                text_marker.pose.position.y = poly.centroid.y
                text_marker.pose.position.z = 0.5  # slightly above the ground
                text_marker.pose.orientation.w = 1.0  # default pose

                # Use label if it exists, otherwise use the URI
                text_marker.text = row.get("label", {}).get(
                    "value", row["room"]["value"]
                )

                marker_array.markers.append(text_marker)

            self.publisher.publish(marker_array)
        except Exception as e:
            self.get_logger().error(f"Failed to process query results: {e}")

    def update_markers_periodically(self):
        # SPARQL query to get all rooms
        sparql = """
        PREFIX ex: <http://example.org/>
        PREFIX geo: <http://www.opengis.net/ont/geosparql#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT ?room ?wkt ?label
        WHERE {
            ?room a ex:Room ;
                  geo:hasGeometry/geo:asWKT ?wkt .
            OPTIONAL { ?room rdfs:label ?label . }
        }
        """

        self.send_sparql_query_to_kb(sparql)


def main(args=None):
    rclpy.init(args=args)
    node = KBPolygonViz()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()
