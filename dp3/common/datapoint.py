from datetime import datetime
from ipaddress import IPv4Address, IPv6Address
from typing import Annotated, Any, Optional, Union

from pydantic import BaseModel, Field, PlainSerializer

from dp3.common.types import T2Datetime


def to_json_friendly(v):
    if isinstance(v, (IPv4Address, IPv6Address)):
        return str(v)
    return v


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
    v: Annotated[Optional[Any], PlainSerializer(to_json_friendly, when_used="json")] = None
    c: Optional[Any] = None
    t1: Optional[Any] = None
    t2: Optional[Any] = None


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
    t2: Optional[T2Datetime] = None
    c: Annotated[float, Field(ge=0.0, le=1.0)] = 1.0


class DataPointTimeseriesBase(DataPointBase):
    """Timeseries attribute data-point

    Contains single raw data value received on API for observations attribute.
    """

    t1: datetime
    t2: Optional[T2Datetime] = None


def is_list_ordered(to_check: list):
    """Checks if list is ordered (not decreasing anywhere)"""
    return all(to_check[i] <= to_check[i + 1] for i in range(len(to_check) - 1))


# Timeseries validators
# They are included by child classes of `DataPointTimeseriesBase` in `AttrSpecTimeseries`,
# where `v` is available
# I know, this is a bit dirty, but there's no other way around.
def dp_ts_v_validator(v):
    """Check if all value arrays are the same length."""
    values_len = {len(v_i) for _, v_i in v.model_dump().items()}
    assert len(values_len) == 1, f"Series values have different lengths: {values_len}"

    return v


def dp_ts_root_validator_regular_wrapper(time_step):
    def dp_ts_root_validator_regular(self):
        """Validates or sets t2 of regular timeseries datapoint"""
        # Get length of first value series included in datapoint
        first_series = list(self.v.model_dump().keys())[0]
        series_len = len(self.v.model_dump()[first_series])
        correct_t2 = self.t1 + series_len * time_step

        if self.t2:
            assert (
                self.t2 == correct_t2
            ), "Difference of t1 and t2 is invalid. Must be values_len*time_step."
        else:
            self.t2 = correct_t2

        return self

    return dp_ts_root_validator_regular


def dp_ts_root_validator_irregular(self):
    """Validates or sets t2 of irregular timeseries datapoint"""
    first_time = self.v.time[0]
    last_time = self.v.time[-1]

    # Check t1 <= first_time
    assert self.t1 <= first_time, f"'t1' is above first item in 'time' series ({first_time})"

    # Check last_time <= t2
    if self.t2:
        assert self.t2 >= last_time, f"'t2' is below last item in 'time' series ({last_time})"
    else:
        self.t2 = last_time

    # time must be ordered
    assert is_list_ordered(self.v.time), "'time' series is not ordered"

    return self


def dp_ts_root_validator_irregular_intervals(self):
    """Validates or sets t2 of irregular intervals timeseries datapoint"""
    first_time = self.v.time_first[0]
    last_time = self.v.time_last[-1]

    # Check t1 <= first_time
    assert self.t1 <= first_time, f"'t1' is above first item in 'time_first' series ({first_time})"

    # Check last_time <= t2
    if self.t2:
        assert self.t2 >= last_time, f"'t2' is below last item in 'time_last' series ({last_time})"
    else:
        self.t2 = last_time

    # Check time_first[i] <= time_last[i]
    assert all(
        t[0] <= t[1] for t in zip(self.v.time_first, self.v.time_last)
    ), "'time_first[i] <= time_last[i]' isn't true for all 'i'"

    return self


DataPointType = Union[DataPointPlainBase, DataPointObservationsBase, DataPointTimeseriesBase]
