"""
Magic strings are used to convert non-JSON-native types to their respective objects.

For querying non-JSON-native types, you can use the following magic strings:

- `"$$IPv4{<ip address>}"` - converts to IPv4Address object
- `"$$IPv6{<ip address>}"` - converts to IPv6Address object
- `"$$int{<value>}"` - may be necessary for filtering when `eid` data type is int
- `"$$Date{<YYYY-mm-ddTHH:MM:ssZ>}"` - converts specified UTC date to UTC datetime object
- `"$$DateTs{<POSIX timestamp>}"` - converts POSIX timestamp (int/float) to UTC datetime object
- `"$$MAC{<mac address>}"` - converts to MACAddress object

To query an IP prefix, use the following magic strings:

- `"$$IPv4Prefix{<ip address>/<prefix length>}"` - matches prefix
- `"$$IPv6Prefix{<ip address>/<prefix length>}"` - matches prefix

To query a binary `_id`s of non-string snapshot buckets,
use the following magic string:

- `"$$Binary_ID{<EID object | Valid magic string>}"`

    - converts to filter the exact EID object snapshots, only EID valid types are supported

There are no attribute name checks (may be added in the future).

Generic filter examples:

- `{"attr1": {"$gt": 10}, "attr2": {"$lt": 20}}`
- `{"ip_attr": "$$IPv4{127.0.0.1}"}` - converts to IPv4Address object, exact match
- `{"ip_attr": "$$IPv4Prefix{127.0.0.0/24}"}`
    - converts to `{"ip_attr": {"$gte": "$$IPv4{127.0.0.0}",
      "$lte": "$$IPv4{127.0.0.255}"}}`

- `{"ip_attr": "$$IPv6{::1}"}` - converts to IPv6Address object, exact match
- `{"ip_attr": "$$IPv6Prefix{::1/64}"}`
    - converts to `{"ip_attr": {"$gte": "$$IPv6{::1}",
      "$lte": "$$IPv6{::1:ffff:ffff:ffff:ffff}"}}`

- `{"_id": "$$Binary_ID{$$IPv4{127.0.0.1}}"}`
    - converts to `{"_id": {"$gte": Binary(<IP bytes + min timestamp>),
      "$lt": Binary(<IP bytes + max timestamp>)}}`

- `{"id": "$$Binary_ID{$$IPv4Prefix{127.0.0.0/24}}"}`
    - converts to `{"_id": {"$gte": Binary(<IP 127.0.0.0 bytes + min timestamp>),
      "$lte": Binary(<IP 127.0.0.255 bytes + max timestamp>)}}`

- `{"_time_created": {"$gte": "$$Date{2024-11-07T00:00:00Z}"}}`
    - converts to `{"_time_created": datetime(2024, 11, 7, 0, 0, 0, tzinfo=timezone.utc)}`

- `{"_time_created": {"$gte": "$$DateTs{1609459200}"}}`
    - converts to `{"_time_created": datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc)}`

- `{"attr": "$$MAC{00:11:22:33:44:55}"}` - converts to MACAddress object, exact match
- `{"_id": "$$Binary_ID{$$MAC{Ab-cD-Ef-12-34-56}}"}`
    - converts to `{"_id": {"$gte": Binary(<MAC bytes + min timestamp>),
        "$lt": Binary(<MAC bytes + max timestamp>)}}`
"""

import re
from datetime import datetime, timezone
from ipaddress import IPv4Address, IPv4Network, IPv6Address, IPv6Network
from typing import Any, Union

from bson import Binary

from dp3.common.mac_address import MACAddress
from dp3.common.utils import int2bytes


def _pack_binary_snapshot_bucket_id(eid: bytes, ts_int: int) -> Binary:
    return Binary(b"".join([eid, ts_int.to_bytes(8, "big", signed=True)]))


def _binary_snapshot_bucket_range(eid: bytes) -> dict[str, Binary]:
    return {
        "$gte": _pack_binary_snapshot_bucket_id(eid, 0),
        "$lt": _pack_binary_snapshot_bucket_id(eid, -1),
    }


def _binary_id_filter(value: Any, _) -> dict[str, Binary]:
    """Converts the value to a binary ID filter."""
    if isinstance(value, str):
        match = magic_regex.match(value)
        if match:
            magic_type, value = match.groups()
            value = magic_string_replacements[magic_type](value, True)

    if isinstance(value, (IPv4Address, IPv6Address, MACAddress)):
        return _binary_snapshot_bucket_range(value.packed)
    if isinstance(value, (IPv4Network, IPv6Network)):
        return {
            "$gte": _pack_binary_snapshot_bucket_id(value[0].packed, 0),
            "$lte": _pack_binary_snapshot_bucket_id(value[-1].packed, -1),
        }
    if isinstance(value, int):
        return _binary_snapshot_bucket_range(int2bytes(value))

    raise ValueError(f"Unsupported value type {type(value)}: {value}")


def _parse_ipv4_network(
    value: str, in_id_filter: bool
) -> Union[IPv4Network, dict[str, IPv4Address]]:
    ip = IPv4Network(value)
    if in_id_filter:
        return ip
    return {"$gte": ip[0], "$lte": ip[-1]}


def _parse_ipv6_network(
    value: str, in_id_filter: bool
) -> Union[IPv6Network, dict[str, IPv6Address]]:
    ip = IPv6Network(value)
    if in_id_filter:
        return ip
    return {"$gte": ip[0], "$lte": ip[-1]}


def _parse_mac_address(value: str, _) -> MACAddress:
    return MACAddress(value)


def _parse_date_ts(value: Union[int, float], _) -> datetime:
    return datetime.fromtimestamp(float(value), timezone.utc)


def _parse_date_string(value: str, _) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


magic_string_replacements = {
    "int": lambda x, _: int(x),
    "IPv4": lambda x, _: IPv4Address(x),
    "IPv6": lambda x, _: IPv6Address(x),
    "Date": _parse_date_string,
    "DateTs": _parse_date_ts,
    "IPv4Prefix": _parse_ipv4_network,
    "IPv6Prefix": _parse_ipv6_network,
    "MAC": _parse_mac_address,
    "Binary_ID": _binary_id_filter,
}

magic_regex = re.compile(r"\$\$(" + "|".join(magic_string_replacements) + r")\{(.+?)}$")


def search_and_replace(query: dict[str, Any]) -> dict[str, Any]:
    """Search and replace all occurrences of magic strings in the query.

    A helper function to
    [`snapshots.get_latest`][dp3.database.snapshots.TypedSnapshotCollection.get_latest]
    """
    if isinstance(query, dict):
        for key, value in query.items():
            if isinstance(value, (dict, list)):
                search_and_replace(value)
            elif isinstance(value, str):
                match = magic_regex.match(value)
                if match:
                    magic_type, magic_value = match.groups()
                    if magic_type in magic_string_replacements:
                        query[key] = magic_string_replacements[magic_type](magic_value, False)
    if isinstance(query, list):
        for item in query:
            search_and_replace(item)
    return query
