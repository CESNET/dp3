from datetime import datetime
from typing import Any, Optional, Union

from pydantic import BaseModel, confloat, root_validator, validator

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

    # Attribute spec just for internal validation. Discarded after that.
    attr_spec: Optional[dict[str, dict[str, Union[EntitySpec, dict[str, Any]]]]] = None

    etype: str
    eid: str
    attr: str
    v: Any
    src: Optional[str] = None
    t1: Optional[datetime] = None
    t2: Optional[datetime] = None
    c: confloat(ge=0.0, le=1.0) = 1.0

    @validator("etype")
    def validate_etype(cls, v, values):
        if "attr_spec" in values:
            assert v in values["attr_spec"], f"Invalid etype '{self.etype}'"
        return v

    @validator("attr")
    def validate_attr(cls, v, values):
        if "attr_spec" in values and "etype" in values:
            assert v in values["attr_spec"][values["etype"]]["attribs"], \
                f"Invalid attribute '{self.attr}'"
        return v

    @validator("v")
    def validate_value(cls, v, values):
        if "attr_spec" in values and "etype" in values and "attr" in values:
            attrib_conf = values["attr_spec"][values["etype"]]["attribs"][values["attr"]]

            # Check value using data type's value_validator()
            if attrib_conf.t in AttrType.PLAIN | AttrType.OBSERVATIONS:
                data_type_obj = attrib_conf.data_type
                assert data_type_obj.value_validator(v, attrib_conf), \
                    f"Invalid value '{v}' (must be data type {data_type_obj.data_type})"

            elif attrib_conf.t == AttrType.TIMESERIES:
                assert type(v) is dict, f"Invalid value '{v}' (must be dictionary of lists)"

                for series in v:
                    assert series in attrib_conf.series, f"Series '{series}' doesn't exist"

                    data_type_obj = attrib_conf.series[series].data_type

                    assert type(v[series]) is list, f"Series '{series}' must be list"
                    for v_i in v[series]:
                        assert data_type_obj.value_validator(v_i, attrib_conf), \
                            f"Invalid value '{v_i}' in series {series} (must be data type {data_type_obj.data_type})"

                # Check all series are present
                for series in attrib_conf.series:
                    assert series in v, f"Series '{series}' is missing in datapoint"

                # Check if all value arrays are the same length
                values_len = [ len(v_i) for _, v_i in v.items() ]
                assert len(set(values_len)) == 1, f"Series values have different lengths: {values_len}"

        return v

    @validator("t1", pre=True, always=True)
    def set_t1_plain(cls, v, values):
        if "attr_spec" in values and "etype" in values and "attr" in values:
            attrib_conf = values["attr_spec"][values["etype"]]["attribs"][values["attr"]]
            if attrib_conf.t == AttrType.PLAIN:
                return datetime.now()
        return v

    @validator("t1", always=True)
    def validate_t1(cls, v, values):
        if "attr_spec" in values and "etype" in values and "attr" in values:
            assert type(v) is datetime, "Field 't1' is missing"
        return v

    @validator("t2", pre=True, always=True)
    def set_t2_plain(cls, v, values):
        if "attr_spec" in values and "etype" in values and "attr" in values:
            attrib_conf = values["attr_spec"][values["etype"]]["attribs"][values["attr"]]
            if attrib_conf.t == AttrType.PLAIN:
                return values["t1"]
        return v

    @validator("t2", always=True)
    def validate_t2(cls, v, values):
        if "attr_spec" in values and "etype" in values and "attr" in values:
            assert type(v) is datetime, "Field 't2' is missing"
            assert values["t1"] <= v, "'t2' is before 't1'"

            attrib_conf = values["attr_spec"][values["etype"]]["attribs"][values["attr"]]
            if attrib_conf.t == AttrType.TIMESERIES and "v" in values:  # `v` won't be in values in case it's check failed
                if attrib_conf.timeseries_type == "regular":
                    time_step = attrib_conf.timeseries_params.time_step
                    assert time_step, "Time step of regular timeseries is not defined. Check your config."

                    # Get length of first value series included in datapoint
                    first_series = list(attrib_conf.series.keys())[0]
                    series_len = len(values["v"][first_series])

                    assert v == values["t1"] + series_len*time_step, \
                        "Difference of t1 and t2 is invalid. Must be values_len*time_step."

                elif attrib_conf.timeseries_type == "irregular":
                    last_time = values["v"]["time"][-1]
                    assert v >= last_time, f"'t2' is below last item in 'time' series ({last_time})"

                elif attrib_conf.timeseries_type == "irregular_intervals":
                    last_time = values["v"]["time_last"][-1]
                    assert v >= last_time, f"'t2' is below last item in 'time_last' series ({last_time})"

        return v

    @root_validator
    def discard_attr_spec(cls, values):
        # This is run at the end of validation.
        # Discard attribute specification.
        del values["attr_spec"]
        return values
