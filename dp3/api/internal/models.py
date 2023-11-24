from datetime import datetime
from typing import Annotated, Any, Optional

from pydantic import BaseModel, Field, model_validator

from dp3.api.internal.helpers import api_to_dp3_datapoint
from dp3.common.types import T2Datetime


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
    v: Any = None
    t1: Optional[datetime] = None
    t2: Optional[T2Datetime] = Field(None, validate_default=True)
    c: Annotated[float, Field(ge=0.0, le=1.0)] = 1.0
    src: Optional[str] = None

    @model_validator(mode="after")
    def validate_against_attribute(self):
        # Try to convert API datapoint to DP3 datapoint
        try:
            api_to_dp3_datapoint(self.model_dump())
        except KeyError as e:
            raise ValueError(f"Missing key: {e}") from e

        return self
