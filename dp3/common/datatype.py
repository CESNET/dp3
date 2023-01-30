from datetime import timedelta
from enum import Flag, auto
from typing import Any, Optional, Callable
import functools
import ipaddress
import re

from pydantic import BaseModel, validator


# List of primitive data types
primitive_data_types = set([
    "tag",
    "binary",
    "string",
    "int",
    "int64",
    "float",
    "ipv4",
    "ipv6",
    "mac",
    "time",
    "special",  # deprecated, use json instead
    "json"
])

# Regular expressions for parsing various data types
re_timestamp = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt ][0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?([Zz]|(?:[+-][0-9]{2}:[0-9]{2}))?$")
re_mac = re.compile(r"^([0-9a-fA-F]{2}[:-]){5}([0-9a-fA-F]{2})$")
re_array = re.compile(r"^array<(\w+)>$")
re_set = re.compile(r"^set<(\w+)>$")
re_link = re.compile(r"^link<(\w+)>$")
re_dict = re.compile(r"^dict<((\w+\??:\w+,)*(\w+\??:\w+))>$")


# Validate ipv4 string
def valid_ipv4(address, attr_spec):
    try:
        ipaddress.IPv4Address(address)
        return True
    except ValueError:
        return False


# Validate ipv6 string
def valid_ipv6(address, attr_spec):
    try:
        ipaddress.IPv6Address(address)
        return True
    except ValueError:
        return False


# Validate timestamp string
def valid_rfc3339(timestamp, attr_spec):
    return re_timestamp.match(timestamp)


# Validate MAC string
def valid_mac(address, attr_spec):
    return re_mac.match(address)


# Dictionary containing validator functions for primitive data types
validators = {
    "tag": lambda v, attr_spec: type(v) is bool,
    "binary": lambda v, attr_spec: type(v) is bool,
    "string": lambda v, attr_spec: type(v) is str,
    "int": lambda v, attr_spec: type(v) is int,
    "int64": lambda v, attr_spec: type(v) is int,
    "float": lambda v, attr_spec: type(v) is float,
    "ipv4": valid_ipv4,
    "ipv6": valid_ipv6,
    "mac": valid_mac,
    "time": valid_rfc3339,
    "special": lambda v, attr_spec: v is not None,
    "json": lambda v, attr_spec: v is not None  # TODO validate json format?
}


# Validate array object
def valid_array(obj, data_type, attr_spec):
    if type(obj) is not list:
        return False
    f = validators[data_type]
    for item in obj:
        if not f(item, attr_spec):
            return False
    return True


# Validate set object
def valid_set(obj, data_type, attr_spec):
    if type(obj) is not list:
        return False
    f = validators[data_type]
    for item in obj:
        if not f(item, attr_spec) or obj.count(item) > 1:
            return False
    return True


# Validate dict object
def valid_dict(obj, key_spec, attr_spec):
    if type(obj) is not dict:
        return False
    for key in key_spec:
        if key not in obj:
            if key[-1] == "?":
                continue
            else:
                return False
        f = validators[key_spec[key]]
        if not f(obj[key], attr_spec):
            return False
    return True


class DataTypeContainer:
    """Data type container

    Represents one of primitive data types:
    - tag
    - binary
    - string
    - int
    - int64
    - float
    - ipv4
    - ipv6
    - mac
    - time
    - special
    - json

    or composite data type:
    - link<data_type>
    - array<data_type>
    - set<data_type>
    - dict<keys>

    Consists of two attributes:
    - `data_type`: data type as string
    - `value_validator` function for incoming value validation
    """

    def __init__(self, data_type: str, value_validator: Callable[[Any, Any], bool]):
        self.data_type = data_type
        self.value_validator = value_validator

    @classmethod
    def __get_validators__(cls):
        # For Pydantic
        yield cls.validate_and_create

    @classmethod
    def validate_and_create(cls, data_type):
        # Validates given data-type and creates instance of
        # `DataTypeContainer`

        value_validator = None

        if type(data_type) is not str:
            raise TypeError(f"Data type {data_type} is not string")

        if data_type in primitive_data_types:
            value_validator = validators[data_type]

        elif data_type == "category":
            value_validator = lambda v, attr_spec: validators["string"](v, attr_spec) if attr_spec.categories is None else v in attr_spec.categories

        elif re.match(re_array, data_type):
            element_type = data_type.split("<")[1].split(">")[0]
            if element_type not in primitive_data_types:
                raise TypeError(f"Data type {element_type} is not supported as an array element")
            value_validator = lambda v, attr_spec: valid_array(v, element_type, attr_spec)

        elif re.match(re_set, data_type):
            element_type = data_type.split("<")[1].split(">")[0]
            if element_type not in primitive_data_types:
                raise TypeError(f"Data type {element_type} is not supported as an set element")
            value_validator = lambda v, attr_spec: valid_set(v, element_type, attr_spec)

        elif re.match(re_link, data_type):
            # TODO
            # Should the entity type be validated here? I.e. does the specification for given entity type have to exist?
            value_validator = lambda v, attr_spec: v is not None

        elif re.match(re_dict, data_type):
            key_str = data_type.split("<")[1].split(">")[0]
            key_spec = dict(item.split(":") for item in key_str.split(","))

            for k in key_spec:
                if key_spec[k] not in primitive_data_types:
                    raise TypeError(f"Data type {element_type} is not supported as an dict field")

            value_validator = lambda v, attr_spec: valid_dict(v, key_spec, attr_spec)

        else:
            raise TypeError(f"Data type '{data_type}' is not supported")

        return cls(data_type, value_validator)

    def __repr__(self):
        return f"'{self.data_type}'"
