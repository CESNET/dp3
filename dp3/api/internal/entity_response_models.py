from datetime import datetime
from typing import Annotated, Any, Optional, Union

from pydantic import BaseModel, Field, NonNegativeInt, PlainSerializer

from dp3.common.attrspec import AttrSpecType, AttrType
from dp3.common.datapoint import to_json_friendly


class EntityState(BaseModel):
    """Entity specification and current state

    Merges (some) data from DP3's `EntitySpec` and state information from `Database`.
    Provides estimate count of master records in database.
    """

    id: str
    id_data_type: str
    name: str
    attribs: dict[str, Annotated[AttrSpecType, Field(discriminator="type")]]
    eid_estimate_count: NonNegativeInt


# This is necessary to allow for non-JSON-serializable types in the model
JsonVal = Annotated[Any, PlainSerializer(to_json_friendly, when_used="json")]

LinkVal = dict[str, JsonVal]
PlainVal = Union[LinkVal, JsonVal]
MultiVal = list[PlainVal]
HistoryVal = list[dict[str, PlainVal]]

Dp3Val = Union[HistoryVal, MultiVal, PlainVal]

EntityEidMasterRecord = dict[str, Dp3Val]

SnapshotType = dict[str, Dp3Val]
EntityEidSnapshots = list[SnapshotType]


class EntityEidList(BaseModel):
    """List of entity eids and their data based on latest snapshot

    Includes timestamp of latest snapshot creation, count of returned documents
    and total count of documents available under specified filter.

    Data does not include history of observations attributes and timeseries.
    """

    time_created: Optional[datetime] = None
    count: int
    total_count: int
    data: EntityEidSnapshots


class EntityEidCount(BaseModel):
    """Total count of documents available under specified filter."""

    total_count: int


class EntityEidData(BaseModel):
    """Data of entity eid

    Includes all snapshots and master record.

    `empty` signalizes whether this eid includes any data.
    """

    empty: bool
    master_record: EntityEidMasterRecord
    snapshots: EntityEidSnapshots


class EntityEidAttrValueOrHistory(BaseModel):
    """Value and/or history of entity attribute for given eid

    Depends on attribute type:
    - plain: just (current) value
    - observations: (current) value and history stored in master record (optionally filtered)
    - timeseries: just history stored in master record (optionally filtered)
    """

    attr_type: AttrType
    current_value: Dp3Val = None
    history: HistoryVal = []


class EntityEidAttrValue(BaseModel):
    """Value of entity attribute for given eid

    The value is fetched from master record.
    """

    value: JsonVal = None
