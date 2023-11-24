from datetime import timedelta
from typing import Annotated

from pydantic import BeforeValidator

from dp3.common.utils import parse_time_duration, time_duration_pattern


def parse_timedelta_or_passthrough(v):
    """
    We pass the value to the native pydantic validator if the value does not match our pattern.
    """
    if v and time_duration_pattern.match(v):
        return parse_time_duration(v)
    return v


ParsedTimedelta = Annotated[timedelta, BeforeValidator(parse_timedelta_or_passthrough)]
