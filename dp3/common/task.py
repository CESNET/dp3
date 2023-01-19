import json

from datetime import datetime
from typing import Any, Optional, Union

from pydantic import BaseModel, validator

from dp3.common.datapoint import DataPoint
from dp3.common.entityspec import EntitySpec

class Task(BaseModel):
    """Task

    Contains single task to be pushed to TaskQueue and processed.
    """

    attr_spec: Optional[dict[str, dict[str, Union[EntitySpec, dict[str, Any]]]]] = None
    etype: str
    ekey: str
    data_points: list[DataPoint] = []
    src: Optional[str] = None
    tags: list[Any] = []
    ttl_token: Optional[datetime] = None

    @validator("data_points")
    def validate_data_points(cls, v, values):
        if values["attr_spec"]:
            for dp in v:
                dp.validate_against_attr_spec(values["etype"], values["attr_spec"])
        return v
