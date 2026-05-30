import json

from geometry_msgs.msg import Point
import rclpy
from rclpy.node import Node
from shapely import wkt
from triplestar_msgs.srv import SPARQLQuery
from visualization_msgs.msg import Marker
from visualization_msgs.msg import MarkerArray


class KBGeometryVisualizer(Node):
    def __init__(self):
        super().__init__('kb_geometry_visualizer')

        self.publisher = self.create_publisher(MarkerArray, 'kb_markers', 10)
        self.timer = self.create_timer(1.0, self.fetch_and_publish_geometries)  # 1 Hz update

        self.get_logger().info('KB Geometry Visualizer node started')

    def request_geometries_from_kb(self, sparql: str):
        client = self.create_client(SPARQLQuery, '/triplestar_core/query')
        if not client.wait_for_service(timeout_sec=1.0):
            self.get_logger().warn('Waiting for /triplestar_core/query service...')
            return

        request = SPARQLQuery.Request()
        request.query = sparql

        self.get_logger().debug(f'Sending SPARQL query:\n{sparql}')

        future = client.call_async(request)
        future.add_done_callback(self.process_geometry_results)

    def process_geometry_results(self, future):
        try:
            response = future.result()
            result_json = response.result
            results = json.loads(result_json)['results']['bindings']

            marker_array = MarkerArray()
            marker_id = 0

            for row in results:
                geom = wkt.loads(row['wkt']['value'])
                entity_uri = row['entity']['value']
                label = row.get('label', {}).get('value', entity_uri)

                # Generate a deterministic color based on the entity URI
                color_hash = hash(entity_uri)
                r = ((color_hash >> 16) & 0xFF) / 255.0
                g = ((color_hash >> 8) & 0xFF) / 255.0
                b = (color_hash & 0xFF) / 255.0

                geom_marker = Marker()
                geom_marker.header.frame_id = 'map'
                geom_marker.header.stamp = self.get_clock().now().to_msg()
                geom_marker.ns = 'kb_geometries'
                geom_marker.id = marker_id
                marker_id += 1
                geom_marker.action = Marker.ADD
                geom_marker.color.r = r
                geom_marker.color.g = g
                geom_marker.color.b = b
                geom_marker.color.a = 1.0
                geom_marker.pose.orientation.w = 1.0

                # Process based on geometry type
                if geom.geom_type == 'Polygon':
                    geom_marker.type = Marker.LINE_STRIP
                    geom_marker.scale.x = 0.05  # line thickness
                    geom_marker.points = [
                        Point(x=pt[0], y=pt[1], z=0.0) for pt in geom.exterior.coords
                    ]
                elif geom.geom_type == 'LineString':
                    geom_marker.type = Marker.LINE_STRIP
                    geom_marker.scale.x = 0.05  # line thickness
                    geom_marker.points = [Point(x=pt[0], y=pt[1], z=0.0) for pt in geom.coords]
                elif geom.geom_type == 'Point':
                    geom_marker.type = Marker.SPHERE
                    geom_marker.scale.x = 0.2
                    geom_marker.scale.y = 0.2
                    geom_marker.scale.z = 0.2
                    geom_marker.pose.position.x = geom.x
                    geom_marker.pose.position.y = geom.y
                    geom_marker.pose.position.z = 0.0
                else:
                    self.get_logger().warn(
                        f"Unsupported geometry type '{geom.geom_type}' for {entity_uri}"
                    )
                    continue

                marker_array.markers.append(geom_marker)

                # Add a text marker for the label or URI
                text_marker = Marker()
                text_marker.header.frame_id = 'map'
                text_marker.header.stamp = geom_marker.header.stamp
                text_marker.ns = 'kb_labels'
                text_marker.id = marker_id
                marker_id += 1
                text_marker.type = Marker.TEXT_VIEW_FACING
                text_marker.action = Marker.ADD
                text_marker.scale.z = 0.3  # text size
                text_marker.color.r = r
                text_marker.color.g = g
                text_marker.color.b = b
                text_marker.color.a = 1.0
                text_marker.pose.position.x = geom.centroid.x
                text_marker.pose.position.y = geom.centroid.y
                text_marker.pose.position.z = 0.5  # slightly above the ground
                text_marker.pose.orientation.w = 1.0
                text_marker.text = label

                marker_array.markers.append(text_marker)

            self.publisher.publish(marker_array)
        except Exception as e:
            self.get_logger().error(f'Failed to process geometry results: {e}')

    def fetch_and_publish_geometries(self):
        # SPARQL query to get geometric entities
        sparql = """
        PREFIX geo: <http://www.opengis.net/ont/geosparql#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT ?entity ?wkt ?label
        WHERE {
            ?entity geo:hasGeometry/geo:asWKT ?wkt .
            OPTIONAL { ?entity rdfs:label ?label . }
        }
        """

        self.request_geometries_from_kb(sparql)


def main(args=None):
    rclpy.init(args=args)
    node = KBGeometryVisualizer()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
