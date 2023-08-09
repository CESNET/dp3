import ipaddress
import re
from datetime import datetime
from enum import Enum
from typing import Any, Union

from pydantic import BaseModel, Extra, Json, PrivateAttr, constr, create_model

# Regular expressions for parsing various data types
re_array = re.compile(r"^array<(\w+)>$")
re_set = re.compile(r"^set<(\w+)>$")
re_link = re.compile(r"^link\s*<\s*(?P<etype>\w+)\s*(?:,\s*(?P<data>\S+)\s*)?>$")
re_dict = re.compile(r"^dict\s*<(\s*(\w+\??:\w+,\s*)*(\w+\??:\w+\s*))>$")
re_category = re.compile(
    r"^category<\s*(?P<type>\w+)\s*;\s*(?P<vals>(?:\s*\w+,\s*)*(?:\s*\w+\s*))>$"
)

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


class DataType(BaseModel):
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
    - category<str_type;category1,category2,...>

    Attributes:
        data_type: type for incoming value validation
        hashable: whether contained data is hashable
        is_link: whether this data type is link
        link_to: if `is_link` is True, what is linked target
    """

    __root__: str
    _data_type = PrivateAttr(None)
    _hashable = PrivateAttr(True)
    _is_link = PrivateAttr(False)
    _link_to = PrivateAttr()
    _link_data = PrivateAttr()

    def __init__(self, **data):
        super().__init__(**data)

        str_type = data["__root__"]

        self._hashable = not (
            "dict" in str_type
            or "set" in str_type
            or "array" in str_type
            or "special" in str_type
            or "json" in str_type
            or "link" in str_type
        )

        self.determine_value_validator(str_type)

    def determine_value_validator(self, str_type: str):
        """Determines value validator (inner `data_type`)

        This is not implemented inside `@validator`, because it apparently doesn't work with
        `__root__` models.
        """
        data_type = None

        if not isinstance(str_type, str):
            raise TypeError(f"Data type {str_type} is not string")

        if str_type in primitive_data_types:
            # Primitive type
            data_type = primitive_data_types[str_type]

        elif re.match(re_array, str_type):
            # Array
            element_type = str_type.split("<")[1].split(">")[0]
            if element_type not in primitive_data_types:
                raise TypeError(f"Data type {element_type} is not supported as an array element")
            data_type = list[primitive_data_types[element_type]]

        elif re.match(re_set, str_type):
            # Set
            element_type = str_type.split("<")[1].split(">")[0]
            if element_type not in primitive_data_types:
                raise TypeError(f"Data type {element_type} is not supported as an set element")
            data_type = list[primitive_data_types[element_type]]  # set is not supported by MongoDB

        elif m := re.match(re_link, str_type):
            # Link
            etype, data = m.group("etype"), m.group("data")
            self._link_to = etype
            self._is_link = True
            self._link_data = bool(data)

            if etype and data:
                value_type = DataType(__root__=data)
                data_type = create_model(
                    f"Link<{data}>", __base__=Link, data=(value_type._data_type, ...)
                )
            else:
                data_type = Link

        elif re.match(re_dict, str_type):
            # Dict
            dict_spec = {}

            key_str = str_type.split("<")[1].split(">")[0]
            key_spec = dict(item.strip().split(":") for item in key_str.split(","))

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
            data_type = create_model(f"{str_type}__inner", **dict_spec)

        elif m := re.match(re_category, str_type):
            # Category
            category_type, category_values = m.group("type"), m.group("vals")

            category_type = DataType(__root__=category_type)
            category_values = [
                category_type._data_type(value.strip()) for value in category_values.split(",")
            ]

            data_type = Enum(f"Category<{category_type}>", {val: val for val in category_values})
        else:
            raise TypeError(f"Data type '{str_type}' is not supported")

        # Set data type
        self._data_type = data_type

    @property
    def data_type(self) -> str:
        return self._data_type

    @property
    def hashable(self) -> bool:
        return self._hashable

    @property
    def is_link(self) -> bool:
        return self._is_link

    @property
    def link_to(self) -> str:
        return self._link_to

    def get_linked_entity(self) -> id:
        """Returns linked entity id. Raises ValueError if DataType is not a link."""
        try:
            return self._link_to
        except AttributeError:
            raise ValueError(f"DataType '{self}' is not a link.") from None

    def link_has_data(self) -> bool:
        """Whether link has data. Raises ValueError if DataType is not a link."""
        try:
            return self._link_data
        except AttributeError:
            raise ValueError(f"DataType '{self}' is not a link.") from None

    def __str__(self):
        return self.__root__

    def __repr__(self):
        return f"'{str(self)}'"
