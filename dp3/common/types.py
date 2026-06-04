from datetime import UTC, datetime, timedelta
from ipaddress import IPv4Address, IPv6Address
from json import JSONEncoder
from typing import Annotated, Any

from event_count_logger import DummyEventGroup, EventGroup
from pydantic import AfterValidator, BeforeValidator, ValidationInfo

from dp3.common.utils import parse_time_duration, time_duration_pattern


def parse_timedelta_or_passthrough(v):
    """
    We pass the value to the native pydantic validator if the value does not match our pattern.
    """
    if v and isinstance(v, str) and time_duration_pattern.match(v):
        return parse_time_duration(v)
    return v


ParsedTimedelta = Annotated[timedelta, BeforeValidator(parse_timedelta_or_passthrough)]


def ensure_timezone_aware(v: datetime | None):
    """Ensure datetime is timezone-aware by defaulting to UTC."""
    if v is None:
        return v
    if v.tzinfo is None:
        return v.replace(tzinfo=UTC)
    return v


AwareDatetime = Annotated[datetime, AfterValidator(ensure_timezone_aware)]


def t2_implicity_t1(v, info: ValidationInfo):
    """If t2 is not specified, it is set to t1."""
    v = v or info.data.get("t1")
    return v


def t2_after_t1(v, info: ValidationInfo):
    """t2 must be after t1"""
    if info.data.get("t1"):
        assert info.data["t1"] <= v, "'t2' is before 't1'"
    return v


T2Datetime = Annotated[
    AwareDatetime,
    BeforeValidator(t2_implicity_t1),
    AfterValidator(t2_after_t1),
]

EventGroupType = EventGroup | DummyEventGroup


class DP3Encoder(JSONEncoder):
    """JSONEncoder to encode python types using DP3 conventions."""

    def default(self, o: Any) -> Any:
        if isinstance(o, datetime):
            return o.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4]
        if isinstance(o, IPv4Address | IPv6Address):
            return str(o)
        return super().default(o)
