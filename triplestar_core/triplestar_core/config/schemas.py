from pathlib import Path
from typing import Dict, List

from pydantic import BaseModel, Field


class KBConfig(BaseModel):
    store_path: Path
    preload_files: List[str] = Field(default_factory=list)
    base_iri: str


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
    insertion_subscribers: Dict[str, InsertionSubscriberConfig] = Field(default_factory=dict)
    query_time_topic_subscribers: Dict[str, QueryTimeTopicSubscriberConfig] = Field(
        default_factory=dict
    )
    query_time_tf_subscribers: Dict[str, QueryTimeTFSubscriberConfig] = Field(default_factory=dict)


class QueryServiceConfig(BaseModel):
    query_file: str
    reasoning: bool = False


class QueryServicesConfig(BaseModel):
    query_services: Dict[str, QueryServiceConfig] = Field(default_factory=dict)
