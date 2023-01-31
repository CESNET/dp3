import functools
import ipaddress
import re
from datetime import datetime, timedelta
from enum import Flag, auto
from typing import Any, Callable, Optional

from pydantic import BaseModel, Json, constr, validator

# Regular expressions for parsing various data types
re_array = re.compile(r"^array<(\w+)>$")
re_set = re.compile(r"^set<(\w+)>$")
re_link = re.compile(r"^link<(\w+)>$")
re_dict = re.compile(r"^dict<((\w+\??:\w+,)*(\w+\??:\w+))>$")

# Dictionary containing validator functions for primitive data types
primitive_data_types = {
    "tag": bool,
    "binary": bool,
    "string": str,
    "int": int,
    "int64": int,
    "float": float,
    "ipv4": ipaddress.IPv4Address,
    "ipv6": ipaddress.IPv6Address,
    "mac": constr(regex=r"^([0-9a-fA-F]{2}[:-]){5}([0-9a-fA-F]{2})$"),
    "time": datetime,
    "special": Any,
    "json": Json[Any],
}


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
    - link<str_type>
    - array<str_type>
    - set<str_type>
    - dict<keys>

    Consists of two attributes:
    - `str_type`: data type as string
    - `data_type` function for incoming value validation
    """

    def __init__(self, str_type: str, data_type: Any):
        self.str_type = str_type
        self.data_type = data_type

    @classmethod
    def __get_validators__(cls):
        # For Pydantic
        yield cls.validate_and_create

    @classmethod
    def validate_and_create(cls, str_type):
        # Validates given data-type and creates instance of
        # `DataTypeContainer`

        data_type = None

        if type(str_type) is not str:
            raise TypeError(f"Data type {str_type} is not string")

        if str_type in primitive_data_types:
            data_type = primitive_data_types[str_type]

        elif str_type == "category":
            data_type = str  # TODO

        elif re.match(re_array, str_type):
            element_type = str_type.split("<")[1].split(">")[0]
            if element_type not in primitive_data_types:
                raise TypeError(f"Data type {element_type} is not supported as an array element")
            data_type = list[primitive_data_types[element_type]]

        elif re.match(re_set, str_type):
            element_type = str_type.split("<")[1].split(">")[0]
            if element_type not in primitive_data_types:
                raise TypeError(f"Data type {element_type} is not supported as an set element")
            data_type = set[primitive_data_types[element_type]]

        elif re.match(re_link, str_type):
            # TODO
            # Should the entity type be validated here? I.e. does the specification for given entity type have to exist?
            data_type = Any

        elif re.match(re_dict, str_type):
            key_str = str_type.split("<")[1].split(">")[0]
            key_spec = dict(item.split(":") for item in key_str.split(","))

            for k in key_spec:
                if key_spec[k] not in primitive_data_types:
                    raise TypeError(f"Data type {element_type} is not supported as an dict field")

            data_type = dict[Any, Any]  # TODO

        else:
            raise TypeError(f"Data type '{str_type}' is not supported")

        return cls(str_type, data_type)

    def __repr__(self):
        return f"'{self.str_type}'"
