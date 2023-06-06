import ipaddress
import re
from datetime import datetime
from typing import Any, Union

from pydantic import BaseModel, Extra, Json, constr, create_model

# Regular expressions for parsing various data types
re_array = re.compile(r"^array<(\w+)>$")
re_set = re.compile(r"^set<(\w+)>$")
re_link = re.compile(r"^link<\s*(?P<etype>\w+)\s*(?:,\s*(?P<data>\S+)\s*)?>$")
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
    "json": Union[Json[Any], dict, list],
}


class Link(BaseModel, extra=Extra.forbid):
    eid: str


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
    - link<entity_type>
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

        self.hashable = not (
            "dict" in str_type
            or "set" in str_type
            or "array" in str_type
            or "special" in str_type
            or "json" in str_type
            or "link" in str_type
        )

        if m := re.match(re_link, self.str_type):
            self._link_to = m.group("etype")
            self._link_data = bool(m.group("data"))

        self.is_link = bool(m)

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
            data_type = list[primitive_data_types[element_type]]  # set is not supported by MongoDB

        elif m := re.match(re_link, str_type):
            etype, data = m.group("etype"), m.group("data")

            if etype and data:
                value_type = cls.validate_and_create(data)
                data_type = create_model(
                    f"Link<{data}>", __base__=Link, data=(value_type.data_type, ...)
                )
            else:
                data_type = Link

        elif re.match(re_dict, str_type):
            dict_spec = {}

            key_str = str_type.split("<")[1].split(">")[0]
            key_spec = dict(item.split(":") for item in key_str.split(","))

            # For each dict key
            for k, v in key_spec.items():
                if v not in primitive_data_types:
                    raise TypeError(f"Data type {v} of key {k} is not supported as a dict field")

                # Optional subattribute
                k_optional = k[-1] == "?"

                if k_optional:
                    # Remove question mark from key
                    k = k[:-1]

                # Set (type, default value) for the key
                dict_spec[k] = (primitive_data_types[v], None if k_optional else ...)

            # Create model for this dict
            data_type = create_model(str_type, **dict_spec)

        else:
            raise TypeError(f"Data type '{str_type}' is not supported")

        return cls(str_type, data_type)

    def get_linked_entity(self) -> str:
        """Returns linked entity id. Raises ValueError if DataType is not a link."""
        try:
            return self._link_to
        except AttributeError:
            raise ValueError(f"DataType '{self.str_type}' is not a link.") from None

    def link_has_data(self) -> bool:
        """Whether link has data. Raises ValueError if DataType is not a link."""
        try:
            return self._link_data
        except AttributeError:
            raise ValueError(f"DataType '{self.str_type}' is not a link.") from None

    def __repr__(self):
        return f"'{self.str_type}'"
