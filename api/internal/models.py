from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, NonNegativeInt, confloat, root_validator, validator

from api.internal.helpers import api_to_dp3_datapoint
from dp3.common.attrspec import AttrSpecType


class DataPoint(BaseModel):
    """Data-point for API

    Contains single raw data value received on API.
    This is generic class for plain, observation and timeseries datapoints.

    Provides front line of validation for this data value.

    This differs slightly compared to `DataPoint` from DP3 in naming of attributes due to historic
    reasons.

    After validation of this schema, datapoint is validated using attribute-specific validator to
    ensure full compilance.
    """

    type: str
    id: str
    attr: str
    v: Any
    t1: Optional[datetime] = None
    t2: Optional[datetime] = None
    c: confloat(ge=0.0, le=1.0) = 1.0
    src: Optional[str] = None

    @validator("t2", always=True)
    def validate_t2(cls, v, values):
        t1 = values.get("t1") or datetime.now()
        v = v or t1
        if "t1" in values:
            assert t1 <= v, "'t2' is before 't1'"
        return v

    @root_validator
    def validate_against_attribute(cls, values):
        # Try to convert API datapoint to DP3 datapoint
        api_to_dp3_datapoint(values)

        return values


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

    Includes timestamp of latest snapshot creation.

    Data does not include history of observations attributes and timeseries.
    """

    time_created: Optional[datetime]
    data: list[dict]
