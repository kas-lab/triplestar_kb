from enum import StrEnum

"""
Canonical config for triplestar's ROS service topology.

Single source of truth for service names/types — imported by both
the node(s) that advertise these services and the CLI that calls them.
"""

QUERY_SERVICE_PREFIX = '/triplestar/query'
SPARQL_SERVICE_NAME = '/triplestar/sparql'

SELECT_SRV_TYPE = 'triplestar_msgs/srv/SelectQuery'
ASK_SRV_TYPE = 'triplestar_msgs/srv/AskQuery'
SPARQL_SRV_TYPE = 'triplestar_msgs/srv/SPARQLQuery'


class ServiceTypeKind(StrEnum):
    SELECT = 'SELECT'
    ASK = 'ASK'
    SPARQL = 'SPARQL'


SERVICE_TYPE_KIND = {
    SELECT_SRV_TYPE: 'SELECT',
    ASK_SRV_TYPE: 'ASK',
    SPARQL_SRV_TYPE: 'SPARQL',
}
