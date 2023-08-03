from datetime import datetime
from typing import Optional

from pydantic import BaseModel, confloat, root_validator, validator


class DataPointBase(BaseModel, use_enum_values=True):
    """Data-point

    Contains single raw data value received on API.
    This is just base class - plain, observation or timeseries datapoints inherit from this class
    (see below).

    Provides front line of validation for this data value.

    Internal usage: inside Task, created by TaskExecutor
    """

    etype: str
    eid: str
    attr: str
    src: Optional[str] = None


class DataPointPlainBase(DataPointBase):
    """Plain attribute data-point

    Contains single raw data value received on API for plain attribute.

    In case of plain data-point, it's not really a data-point, but we use
    the same naming for simplicity.
    """


class DataPointObservationsBase(DataPointBase):
    """Observations attribute data-point

    Contains single raw data value received on API for observations attribute.
    """

    t1: datetime
    t2: Optional[datetime] = None
    c: confloat(ge=0.0, le=1.0) = 1.0

    @validator("t2", always=True)
    def validate_t2(cls, v, values):
        v = v or values.get("t1") or datetime.now()
        if "t1" in values:
            assert values["t1"] <= v, "'t2' is before 't1'"
        return v


class DataPointTimeseriesBase(DataPointBase):
    """Timeseries attribute data-point

    Contains single raw data value received on API for observations attribute.
    """

    t1: datetime
    t2: Optional[datetime] = None

    @validator("t2")
    def validate_t2(cls, v, values):
        if "t1" in values:
            assert values["t1"] <= v, "'t2' is before 't1'"
        return v


def is_list_ordered(to_check: list):
    """Checks if list is ordered (not decreasing anywhere)"""
    return all(to_check[i] <= to_check[i + 1] for i in range(len(to_check) - 1))


# Timeseries validators
# They are included by child classes of `DataPointTimeseriesBase` in `AttrSpecTimeseries`,
# where `v` is available
# I know, this is very dirty, but there's no other way around.
@validator("v")
def dp_ts_v_validator(cls, v, values):
    # Check if all value arrays are the same length
    values_len = [len(v_i) for _, v_i in v.dict().items()]
    assert len(set(values_len)) == 1, f"Series values have different lengths: {values_len}"

    return v


def dp_ts_root_validator_regular_wrapper(time_step):
    def dp_ts_root_validator_regular(cls, values):
        """Validates or sets t2 of regular timeseries datapoint"""
        if "v" in values and "t1" in values:
            # Get length of first value series included in datapoint
            first_series = list(values["v"].dict().keys())[0]
            series_len = len(values["v"].dict()[first_series])
            correct_t2 = values["t1"] + series_len * time_step

            if "t2" in values and values["t2"]:
                assert (
                    values["t2"] == correct_t2
                ), "Difference of t1 and t2 is invalid. Must be values_len*time_step."
            else:
                values["t2"] = correct_t2

        return values

    return root_validator(dp_ts_root_validator_regular, allow_reuse=True)


@root_validator
def dp_ts_root_validator_irregular(cls, values):
    """Validates or sets t2 of irregular timeseries datapoint"""
    if "v" in values:
        first_time = values["v"].time[0]
        last_time = values["v"].time[-1]

        # Check t1 <= first_time
        if "t1" in values:
            assert (
                values["t1"] <= first_time
            ), f"'t1' is above first item in 'time' series ({first_time})"

        # Check last_time <= t2
        if "t2" in values and values["t2"]:
            assert (
                values["t2"] >= last_time
            ), f"'t2' is below last item in 'time' series ({last_time})"
        else:
            values["t2"] = last_time

        # time must be ordered
        assert is_list_ordered(values["v"].time), "'time' series is not ordered"

    return values


@root_validator
def dp_ts_root_validator_irregular_intervals(cls, values):
    """Validates or sets t2 of irregular intervals timeseries datapoint"""
    if "v" in values:
        first_time = values["v"].time_first[0]
        last_time = values["v"].time_last[-1]

        # Check t1 <= first_time
        if "t1" in values:
            assert (
                values["t1"] <= first_time
            ), f"'t1' is above first item in 'time_first' series ({first_time})"

        # Check last_time <= t2
        if "t2" in values and values["t2"]:
            assert (
                values["t2"] >= last_time
            ), f"'t2' is below last item in 'time_last' series ({last_time})"
        else:
            values["t2"] = last_time

        # Check time_first[i] <= time_last[i]
        assert all(
            t[0] <= t[1] for t in zip(values["v"].time_first, values["v"].time_last)
        ), "'time_first[i] <= time_last[i]' isn't true for all 'i'"

    return values
