from pathlib import Path

from pydantic import ValidationError
import pytest
from triplestar_core.config import TriplestarConfig


def _config_data() -> dict:
    return {
        'knowledge_base': {
            'store_path': '/tmp/triplestar_kb',
            'base_iri': 'http://triplestar.local',
        },
        'insertion_subscribers': [{'topic': '/detections', 'template': 'detections.sparql.tmpl'}],
        'insertion_services': [
            {
                'service': '/robot/set_enabled',
                'template': 'set-enabled.sparql.tmpl',
                'timeout_sec': 1.5,
            }
        ],
        'query_time_topic_subscribers': [
            {
                'topic': '/clock',
                'sparql_fn_name': 'rosTime',
                'target_msg_field': 'clock',
            }
        ],
        'query_time_tf_subscribers': [
            {
                'from_frame': 'base_link',
                'to_frame': 'map',
                'sparql_fn_name': 'robotPose',
            }
        ],
        'query_services': [
            {'query_file': 'count_triples.sparql', 'service_name': 'count_triples'}
        ],
    }


def test_unified_config_parses_lists():
    config = TriplestarConfig.parse_obj(_config_data())

    assert config.knowledge_base.store_path == Path('/tmp/triplestar_kb')
    assert config.insertion_subscribers[0].topic == '/detections'
    assert config.insertion_services[0].service == '/robot/set_enabled'
    assert config.insertion_services[0].timeout_sec == 1.5
    assert config.query_time_topic_subscribers[0].sparql_fn_name == 'rosTime'
    assert config.query_time_tf_subscribers[0].sparql_fn_name == 'robotPose'
    assert config.query_services[0].service_name == 'count_triples'


def test_optional_sections_default_to_empty_lists():
    config = TriplestarConfig.parse_obj(
        {
            'knowledge_base': {
                'store_path': '',
                'base_iri': 'http://triplestar.local',
            }
        }
    )

    assert config.insertion_subscribers == []
    assert config.insertion_services == []
    assert config.query_time_topic_subscribers == []
    assert config.query_time_tf_subscribers == []
    assert config.query_services == []


def test_query_time_function_names_must_be_unique():
    data = _config_data()
    data['query_time_tf_subscribers'][0]['sparql_fn_name'] = 'rosTime'

    with pytest.raises(ValidationError, match='SPARQL function names must be unique'):
        TriplestarConfig.parse_obj(data)


@pytest.mark.parametrize(
    ('section', 'index'),
    [
        ('query_time_topic_subscribers', 0),
        ('query_time_tf_subscribers', 0),
    ],
)
def test_tf_position_query_time_function_name_is_reserved(section, index):
    data = _config_data()
    data[section][index]['sparql_fn_name'] = 'tfPosition'

    with pytest.raises(ValidationError, match='SPARQL function names are reserved: tfPosition'):
        TriplestarConfig.parse_obj(data)


def test_query_service_names_must_be_unique():
    data = _config_data()
    data['query_services'].append(
        {'query_file': 'get_all_triples.sparql', 'service_name': 'count_triples'}
    )

    with pytest.raises(ValidationError, match='Query service names must be unique'):
        TriplestarConfig.parse_obj(data)


def test_insertion_service_names_must_be_unique():
    data = _config_data()
    data['insertion_services'].append(
        {
            'service': '/robot/set_enabled',
            'template': 'another.sparql.tmpl',
        }
    )

    with pytest.raises(ValidationError, match='Insertion service names must be unique'):
        TriplestarConfig.parse_obj(data)


@pytest.mark.parametrize(
    ('field', 'value', 'message'),
    [
        ('timeout_sec', 0, 'greater than 0'),
        ('service', '/', 'must name a ROS service'),
        ('service', '  ', 'must not be blank'),
        ('service', 'bad service', 'invalid ROS service name'),
        ('template', '', 'must not be blank'),
        ('service_type', 'std_srvs/srv/SetBool', 'extra fields not permitted'),
    ],
)
def test_insertion_service_fields_are_validated(field, value, message):
    data = _config_data()
    data['insertion_services'][0][field] = value

    with pytest.raises(ValidationError, match=message):
        TriplestarConfig.parse_obj(data)
