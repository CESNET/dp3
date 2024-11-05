from typing import Literal, Union

from pydantic import BaseModel, Extra, Field

from dp3.common.datatype import EidDataType
from dp3.common.types import ParsedTimedelta


class SpecModel(BaseModel, extra=Extra.forbid): ...


class ImmortalLifetime(SpecModel):
    """Immortal lifetime specification.

    The entity is never deleted.
    """

    type: Literal["immortal"]


class TimeToLiveLifetime(SpecModel):
    """TTL lifetime specification.

    The entity is deleted after all of its TTL tokens expire.
    TTL tokens can be attached to new data datapoints on per-attribute basis
    (see [`AttrSpecGeneric`][dp3.common.attrspec.AttrSpecGeneric]),
    set to mirror the lifetime of the data (`mirror_data`),
    or sent explicitly using the API (see `/entity/{etype}/{eid}/ttl`).

    Attributes:
        on_create: The base lifetime of an entity.
        mirror_data: If `True` (default), the lifetime of the entity is extended by the
            `max_age` of the incoming observations and timeseries data-points.
    """

    type: Literal["ttl"]
    on_create: ParsedTimedelta
    mirror_data: bool = True


class WeakLifetime(SpecModel):
    """Weak entity lifetime specification"""

    type: Literal["weak"]


class EntitySpec(SpecModel):
    """Entity specification

    This class represents specification of an entity type (e.g. ip, asn, ...)
    """

    id: str
    name: str
    data_type: EidDataType = EidDataType("string")
    snapshot: bool
    lifetime: Union[ImmortalLifetime, TimeToLiveLifetime, WeakLifetime] = Field(
        default_factory=lambda: ImmortalLifetime(type="immortal"), discriminator="type"
    )

    description: str = ""
