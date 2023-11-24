from datetime import datetime, timedelta
from typing import Annotated

from pydantic import AfterValidator, BeforeValidator
from pydantic_core.core_schema import FieldValidationInfo

from dp3.common.utils import parse_time_duration, time_duration_pattern


def parse_timedelta_or_passthrough(v):
    """
    We pass the value to the native pydantic validator if the value does not match our pattern.
    """
    if v and time_duration_pattern.match(v):
        return parse_time_duration(v)
    return v


ParsedTimedelta = Annotated[timedelta, BeforeValidator(parse_timedelta_or_passthrough)]


def validate_t2(v, info: FieldValidationInfo):
    """t2 must be after t1, If t2 is not specified, it is set to t1."""
    v = v or info.data.get("t1")
    if "t1" in info.data:
        assert info.data["t1"] <= v, "'t2' is before 't1'"
    return v


T2Datetime = Annotated[datetime, AfterValidator(validate_t2)]
