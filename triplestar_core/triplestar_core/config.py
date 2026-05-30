from pathlib import Path

from pydantic import BaseModel
from pydantic import Field


class KBConfig(BaseModel):
    store_path: Path
    preload_files: list[str] = Field(default_factory=list)
    base_iri: str
    clear_on_startup: bool = True


class InsertionSubscriberConfig(BaseModel):
    topic: str
    template: str


class QueryTimeTopicSubscriberConfig(BaseModel):
    topic: str
    msg_field_name: str | None = None


class QueryTimeTFSubscriberConfig(BaseModel):
    from_frame: str
    to_frame: str


class SubscribersConfig(BaseModel):
    insertion_subscribers: dict[str, InsertionSubscriberConfig] = Field(default_factory=dict)
    query_time_topic_subscribers: dict[str, QueryTimeTopicSubscriberConfig] = Field(
        default_factory=dict
    )
    query_time_tf_subscribers: dict[str, QueryTimeTFSubscriberConfig] = Field(default_factory=dict)


class QueryServiceConfig(BaseModel):
    query_file: str
    reasoning: bool = False


class QueryServicesConfig(BaseModel):
    query_services: dict[str, QueryServiceConfig] = Field(default_factory=dict)
