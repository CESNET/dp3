from datetime import datetime, timedelta
from ipaddress import IPv4Address, IPv6Address
from json import JSONEncoder
from typing import Annotated, Any, Union

from event_count_logger import DummyEventGroup, EventGroup
from pydantic import AfterValidator, BeforeValidator
from pydantic_core.core_schema import FieldValidationInfo

from dp3.common.utils import parse_time_duration, time_duration_pattern


def parse_timedelta_or_passthrough(v):
    """
    We pass the value to the native pydantic validator if the value does not match our pattern.
    """
    if v and isinstance(v, str) and time_duration_pattern.match(v):
        return parse_time_duration(v)
    return v


ParsedTimedelta = Annotated[timedelta, BeforeValidator(parse_timedelta_or_passthrough)]


def t2_implicity_t1(v, info: FieldValidationInfo):
    """If t2 is not specified, it is set to t1."""
    v = v or info.data.get("t1")
    return v


def t2_after_t1(v, info: FieldValidationInfo):
    """t2 must be after t1"""
    if info.data.get("t1"):
        assert info.data["t1"] <= v, "'t2' is before 't1'"
    return v


T2Datetime = Annotated[datetime, BeforeValidator(t2_implicity_t1), AfterValidator(t2_after_t1)]

EventGroupType = Union[EventGroup, DummyEventGroup]


class DP3Encoder(JSONEncoder):
    """JSONEncoder to encode python types using DP3 conventions."""

    def default(self, o: Any) -> Any:
        if isinstance(o, datetime):
            return o.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4]
        if isinstance(o, (IPv4Address, IPv6Address)):
            return str(o)
        return super().default(o)
