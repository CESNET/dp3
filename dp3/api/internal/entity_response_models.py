from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, NonNegativeInt

from dp3.common.attrspec import AttrSpecType, AttrType


class EntityState(BaseModel):
    """Entity specification and current state

    Merges (some) data from DP3's `EntitySpec` and state information from `Database`.
    Provides estimate count of master records in database.
    """

    id: str
    name: str
    attribs: dict[str, AttrSpecType]
    eid_estimate_count: NonNegativeInt


class EntityEidList(BaseModel):
    """List of entity eids and their data based on latest snapshot

    Includes timestamp of latest snapshot creation, count of returned documents
    and total count of documents available under specified filter.

    Data does not include history of observations attributes and timeseries.
    """

    time_created: Optional[datetime]
    count: int
    total_count: int
    data: list[dict]


class EntityEidData(BaseModel):
    """Data of entity eid

    Includes all snapshots and master record.

    `empty` signalizes whether this eid includes any data.
    """

    empty: bool
    master_record: dict
    snapshots: list[dict]


class EntityEidAttrValueOrHistory(BaseModel):
    """Value and/or history of entity attribute for given eid

    Depends on attribute type:
    - plain: just (current) value
    - observations: (current) value and history stored in master record (optionally filtered)
    - timeseries: just history stored in master record (optionally filtered)
    """

    attr_type: AttrType
    current_value: Any = None
    history: list[dict] = []


class EntityEidAttrValue(BaseModel):
    """Value of entity attribute for given eid

    The value is fetched from master record.
    """

    value: Any
