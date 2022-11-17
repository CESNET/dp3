from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, confloat

from dp3.common.attrspec import AttrSpec, AttrType


class DataPoint(BaseModel):
    """Data-point

    Contains single raw data value received on API - plain, observation or
    timeseries. In case of plain data-point, it's not really a data-point, but
    we use the same class for simplicity.

    Provides front line of validation for this data value.

    Internal usage: inside Task, created by TaskExecutor
    """

    attr_type: AttrType
    etype: str
    eid: str
    attr: str
    v: Any
    src: Optional[str] = None
    t1: datetime
    t2: datetime
    c: Optional[confloat(ge=0.0, le=1.0)] = 1.0

    def __init__(self, attr_type: AttrType, **data):
        # Plain attributes don't have any t1 and t2.
        # Give this fields any value, so it passes validation.
        if attr_type == AttrType.PLAIN:
            default_values = {
                "t1": datetime.now(),
                "t2": datetime.now()
            }
        else:
            default_values = {}

        # Merge data with default values
        data = default_values | data

        super().__init__(attr_type=attr_type, **data)
