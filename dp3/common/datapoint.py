from datetime import datetime
from typing import Any, Optional, Union

from pydantic import BaseModel, confloat

from dp3.common.attrspec import AttrSpecGeneric, AttrType
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
        # Let be t1 and t2 current timestamps, as the change was made now.
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

    def validate_against_attr_spec(self, etype: str, attr_spec: dict[str, dict[str, Union[EntitySpec, dict[str, AttrSpecGeneric]]]]):
        """Validates self against provided attributes specification

        This method is called when creating Task.
        """
        assert self.etype == etype, \
            f"etype of task and contained datapoint don't match: {self.etype} != {etype}"
        assert self.etype in attr_spec, f"Invalid etype '{self.etype}'"
        assert self.attr in attr_spec[self.etype]["attribs"], \
            f"Invalid attribute '{self.attr}' of entity '{self.etype}'"

        attrib_spec = attr_spec[self.etype]["attribs"][self.attr]

        assert self.attr_type == attrib_spec.t, \
            f"Invalid attribute type: DP has '{self.attr_type}', but attribute '{self.attr}' of entity '{self.etype}' has {attrib_spec.t}"

        if self.attr_type == AttrType.TIMESERIES:
            # Check if all value arrays are the same length
            values_len = [ len(v_i) for _, v_i in self.v.items() ]
            assert len(set(values_len)) == 1, f"Datapoint arrays have different lengths: {values_len}"

            # Check t2
            if attrib_spec.timeseries_type == "regular":
                time_step = attrib_spec.timeseries_params.time_step
                assert time_step, "Time step of regular timeseries is not defined. Check your config."
                assert self.t2 - self.t1 == values_len[0] * time_step, \
                    f"Difference of t1 and t2 is invalid. Must be values_len*time_step."

            # Check all series are present
            for series_id in attrib_spec.series:
                assert series_id in self.v, f"Datapoint is missing values for '{series_id}' series"

        # Validate value
        if self.attr_type in AttrType.PLAIN | AttrType.OBSERVATIONS:
            data_type_obj = attrib_spec.data_type
            assert data_type_obj.value_validator(self.v), \
                f"Datapoint for {self.etype} '{self.eid}', attr '{self.attr}' contains invalid value '{self.v}' (must be data type {data_type_obj.data_type})"

        elif self.attr_type == AttrType.TIMESERIES:
            for series in self.v:
                data_type_obj = attrib_spec.series[series].data_type
                assert type(self.v[series]) is list, \
                    f"Series '{series}' in datapoint for {self.etype} '{self.eid}', attr '{self.attr}' must be list."
                for v in self.v[series]:
                    assert data_type_obj.value_validator(v), \
                        f"Datapoint for {self.etype} '{self.eid}', attr '{self.attr}' contains invalid value '{v}' in series {series} (must be data type {data_type_obj.data_type})"
