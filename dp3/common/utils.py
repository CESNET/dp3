"""
auxiliary/utility functions and classes
"""

import datetime
import re
from collections.abc import Iterable, Iterator
from functools import partial
from itertools import islice
from typing import Union

# *** IP conversion functions ***
ipv4_re = re.compile(r"^([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})$")


def entity_expired(utcnow: datetime, master_document: dict):
    """Check if entity is expired (all TTLs are in the past)"""
    if "#ttl" not in master_document:
        return False
    return all(ttl < utcnow for ttl in master_document["#ttl"].values())


def ipstr2int(s):
    res = ipv4_re.match(s)
    if res is None:
        raise ValueError(f"Invalid IPv4 format: {s!r}")
    a1, a2, a3, a4 = res.groups()
    # Check if octets are between 0 and 255 is omitted for better performance
    return int(a1) << 24 | int(a2) << 16 | int(a3) << 8 | int(a4)


def int2ipstr(i):
    return ".".join((str(i >> 24), str((i >> 16) & 0xFF), str((i >> 8) & 0xFF), str(i & 0xFF)))


def int2bytes(num: int) -> bytes:
    """Convert signed int to however many bytes necessary in big-endian order"""
    return num.to_bytes(
        length=(8 + (num + (num < 0)).bit_length()) // 8, byteorder="big", signed=True
    )


def bytes2int(b: bytes) -> int:
    """Convert bytes to signed int in big-endian order"""
    return int.from_bytes(b, "big", signed=True)


# *** Time conversion ***
# Regex for RFC 3339 time format
timestamp_re = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})[Tt ](\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?([Zz]|(?:[+-]\d{2}:\d{2}))?$"
)


def parse_rfc_time(time_str):
    """
    Parse time in RFC 3339 format and return it as naive datetime in UTC.

    Timezone specification is optional (UTC is assumed when none is specified).
    """
    res = timestamp_re.match(time_str)
    if res is not None:
        year, month, day, hour, minute, second = (int(n or 0) for n in res.group(*range(1, 7)))
        us_str = (res.group(7) or "0")[:6].ljust(6, "0")
        us = int(us_str)
        zonestr = res.group(8)
        zoneoffset = 0 if zonestr in (None, "z", "Z") else int(zonestr[:3]) * 60 + int(zonestr[4:6])
        zonediff = datetime.timedelta(minutes=zoneoffset)
        return datetime.datetime(year, month, day, hour, minute, second, us) - zonediff
    else:
        raise ValueError("Wrong timestamp format")


time_duration_pattern = re.compile(r"^\s*(\d+)([smhd])?$")


def parse_time_duration(duration_string: Union[str, int, datetime.timedelta]) -> datetime.timedelta:
    """
    Parse duration in format <num><s/m/h/d> (or just "0").

    Return datetime.timedelta
    """
    # if it's already timedelta, just return it unchanged
    if isinstance(duration_string, datetime.timedelta):
        return duration_string
    # if number is passed, consider it number of seconds
    if isinstance(duration_string, (int, float)):
        return datetime.timedelta(seconds=duration_string)

    d = 0
    h = 0
    m = 0
    s = 0

    if duration_string == "0":
        pass
    elif duration_string[-1] == "d":
        d = int(duration_string[:-1])
    elif duration_string[-1] == "h":
        h = int(duration_string[:-1])
    elif duration_string[-1] == "m":
        m = int(duration_string[:-1])
    elif duration_string[-1] == "s":
        s = int(duration_string[:-1])
    else:
        raise ValueError("Invalid time duration string")

    return datetime.timedelta(days=d, hours=h, minutes=m, seconds=s)


# *** object (de)serialization ***
# Functions that allow to (de)serialize some objects we need to pass for example via TaskQueue.
# Inspired by bson.json_util, but for different set of types (and much simpler).
def conv_to_json(obj):
    """Convert special types to JSON (use as "default" param of json.dumps)

    Supported types/objects:
    - datetime
    - timedelta
    """
    if isinstance(obj, datetime.datetime):
        if obj.tzinfo:
            raise NotImplementedError(
                "Can't serialize timezone-aware datetime object "
                "(DP3 policy is to use naive datetimes in UTC everywhere)"
            )
        return {"$datetime": obj.strftime("%Y-%m-%dT%H:%M:%S.%f")}
    if isinstance(obj, datetime.timedelta):
        return {"$timedelta": f"{obj.days},{obj.seconds},{obj.microseconds}"}
    raise TypeError(f"{repr(obj)}%r is not JSON serializable")


def conv_from_json(dct):
    """Convert special JSON keys created by conv_to_json back to Python objects
    (use as "object_hook" param of json.loads)

    Supported types/objects:
    - datetime
    - timedelta
    """
    if "$datetime" in dct:
        val = dct["$datetime"]
        return datetime.datetime.strptime(val, "%Y-%m-%dT%H:%M:%S.%f")
    if "$timedelta" in dct:
        days, seconds, microseconds = dct["$timedelta"].split(",")
        return datetime.timedelta(int(days), int(seconds), int(microseconds))
    return dct


def batched(iterable: Iterable, n: int) -> Iterator[list]:
    """Batch data into tuples of length n. The last batch may be shorter."""
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)
    while True:
        batch = list(islice(it, n))
        if not batch:
            break
        yield batch


# *** pretty print ***
def get_func_name(func_or_method):
    """Get name of function or method as pretty string."""
    if isinstance(func_or_method, partial):
        wrapper = "partial({name}, {args})"
        args = [str(a) for a in func_or_method.args]
        args.extend(f"{k}={v}" for k, v in func_or_method.keywords.items())
        args = ", ".join(args)
        func_or_method = func_or_method.func
    else:
        wrapper = "{name}{args}"
        args = ""

    try:
        fname = func_or_method.__func__.__qualname__
    except AttributeError:
        try:
            fname = func_or_method.__name__
        except AttributeError:
            fname = str(func_or_method)

    try:
        module = func_or_method.__module__
    except AttributeError:
        return fname
    return wrapper.format(name=f"{module}.{fname}", args=args)
