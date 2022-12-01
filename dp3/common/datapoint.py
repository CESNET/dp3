from datetime import datetime
from typing import Any, Optional, Union

from pydantic import BaseModel, confloat

from dp3.common.attrspec import AttrSpec, AttrType
from dp3.common.entityspec import EntitySpec


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

    def validate_against_attr_spec(self, attr_spec: dict[str, dict[str, Union[EntitySpec, dict[str, AttrSpec]]]]):
        """Validates self against provided attributes specification

        This method is called when creating Task.
        """
        assert self.etype in attr_spec, f"Invalid etype '{self.etype}'"
        assert self.attr in attr_spec[self.etype]["attribs"], \
            f"Invalid attribute '{self.attr}' of entity '{self.etype}'"

        attrib_spec = attr_spec[self.etype]["attribs"][self.attr]

        assert self.attr_type == attrib_spec.t, \
            f"Invalid attribute type: DP has '{self.attr_type}', but attribute '{self.attr}' of entity '{self.etype}' has {attrib_spec.t}"
