from pathlib import Path

from pydantic import BaseModel
from pydantic import Field
from pydantic import root_validator
from pydantic import validator
from rclpy.exceptions import InvalidServiceNameException
from rclpy.validate_topic_name import validate_topic_name

TF_POSITION_FUNCTION_NAME = 'tfPosition'
RESERVED_QUERY_TIME_FUNCTION_NAMES = {TF_POSITION_FUNCTION_NAME}


class KBConfig(BaseModel):
    store_path: Path
    preload_files: list[str] = Field(default_factory=list)
    base_iri: str
    clear_on_startup: bool = True


class InsertionSubscriberConfig(BaseModel):
    topic: str
    template: str


class InsertionServiceConfig(BaseModel):
    service: str
    template: str
    timeout_sec: float = Field(default=2.0, gt=0)

    @validator('service', 'template')
    @classmethod
    def _not_blank(cls, value: str, field) -> str:
        value = value.strip()
        if not value:
            raise ValueError(f'{field.name} must not be blank')
        if field.name == 'service':
            if value == '/':
                raise ValueError('service must name a ROS service')
            try:
                validate_topic_name(value, is_service=True)
            except InvalidServiceNameException as e:
                raise ValueError(f'invalid ROS service name: {e}') from e
        return value

    class Config:
        extra = 'forbid'


class QueryTimeTopicSubscriberConfig(BaseModel):
    topic: str
    sparql_fn_name: str
    target_msg_field: str | None = None


class QueryTimeTFSubscriberConfig(BaseModel):
    from_frame: str
    to_frame: str
    sparql_fn_name: str


class QueryServiceConfig(BaseModel):
    query_file: str
    service_name: str


class TriplestarConfig(BaseModel):
    knowledge_base: KBConfig
    insertion_subscribers: list[InsertionSubscriberConfig] = Field(default_factory=list)
    insertion_services: list[InsertionServiceConfig] = Field(default_factory=list)
    query_time_topic_subscribers: list[QueryTimeTopicSubscriberConfig] = Field(
        default_factory=list
    )
    query_time_tf_subscribers: list[QueryTimeTFSubscriberConfig] = Field(default_factory=list)
    query_services: list[QueryServiceConfig] = Field(default_factory=list)

    @validator(
        'insertion_subscribers',
        'insertion_services',
        'query_time_topic_subscribers',
        'query_time_tf_subscribers',
        'query_services',
        pre=True,
    )
    @classmethod
    def _none_to_empty_list(cls, value):
        """Treat an empty YAML key (parsed as None) the same as an omitted one."""
        return value if value is not None else []

    @root_validator
    def _unique_names(cls, values):  # noqa: N805
        query_time_names = [
            subscriber.sparql_fn_name
            for subscriber in values.get('query_time_topic_subscribers', [])
        ] + [
            subscriber.sparql_fn_name for subscriber in values.get('query_time_tf_subscribers', [])
        ]
        if len(query_time_names) != len(set(query_time_names)):
            raise ValueError('Query-time SPARQL function names must be unique')

        reserved_names = sorted(set(query_time_names) & RESERVED_QUERY_TIME_FUNCTION_NAMES)
        if reserved_names:
            raise ValueError(
                'Query-time SPARQL function names are reserved: '
                f'{", ".join(reserved_names)}'
            )

        insertion_service_names = [
            service.service for service in values.get('insertion_services', [])
        ]
        if len(insertion_service_names) != len(set(insertion_service_names)):
            raise ValueError('Insertion service names must be unique')

        service_names = [service.service_name for service in values.get('query_services', [])]
        if len(service_names) != len(set(service_names)):
            raise ValueError('Query service names must be unique')

        return values
