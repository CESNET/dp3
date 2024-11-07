import urllib
from typing import Literal, Union

from pydantic import BaseModel, Field, field_validator


class MongoHostConfig(BaseModel, extra="forbid"):
    """MongoDB host."""

    address: str = "localhost"
    port: int = 27017


class MongoStandaloneConfig(BaseModel, extra="forbid"):
    """MongoDB standalone configuration."""

    mode: Literal["standalone"]
    host: MongoHostConfig = MongoHostConfig()


class MongoReplicaConfig(BaseModel, extra="forbid"):
    """MongoDB replica set configuration."""

    mode: Literal["replica"]
    replica_set: str = "dp3"
    hosts: list[MongoHostConfig]


class StorageConfig(BaseModel, extra="forbid"):
    """Storage configuration."""

    snapshot_bucket_size: int = 32


class MongoConfig(BaseModel, extra="forbid"):
    """Database configuration."""

    db_name: str = "dp3"
    username: str = "dp3"
    password: str = "dp3"
    connection: Union[MongoStandaloneConfig, MongoReplicaConfig] = Field(..., discriminator="mode")
    storage: StorageConfig = StorageConfig()

    @field_validator("username", "password")
    @classmethod
    def url_safety(cls, v):
        return urllib.parse.quote_plus(v)
