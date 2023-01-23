import json

from datetime import datetime
from typing import Any, Optional, Union

from pydantic import BaseModel, root_validator, validator

from dp3.common.datapoint import DataPoint
from dp3.common.entityspec import EntitySpec

class Task(BaseModel):
    """Task

    Contains single task to be pushed to TaskQueue and processed.
    """

    # Attribute spec just for internal validation. Discarded after that.
    attr_spec: Optional[dict[str, dict[str, Union[EntitySpec, dict[str, Any]]]]] = None

    etype: str
    ekey: str
    data_points: list[DataPoint] = []
    tags: list[Any] = []
    ttl_token: Optional[datetime] = None

    @validator("data_points", pre=True, each_item=True)
    def validate_data_points(cls, v, values):
        # Add context to each datapoint
        if type(v) is dict:
            v |= {"attr_spec": values["attr_spec"]}
        return v

    @root_validator
    def discard_attr_spec(cls, values):
        # This is run at the end of validation.
        # Discard attribute specification.
        del values["attr_spec"]
        return values
