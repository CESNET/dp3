from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, confloat, root_validator, validator

from dp3.api.internal.helpers import api_to_dp3_datapoint


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

    @root_validator(skip_on_failure=True)
    def validate_against_attribute(cls, values):
        # Try to convert API datapoint to DP3 datapoint
        try:
            api_to_dp3_datapoint(values)
        except KeyError as e:
            raise ValueError(f"Missing key: {e}") from e

        return values
