import ipaddress
import re
from datetime import datetime
from typing import Any, Union

from pydantic import BaseModel, Json, PrivateAttr, constr, create_model

# Regular expressions for parsing various data types
re_array = re.compile(r"^array<(\w+)>$")
re_set = re.compile(r"^set<(\w+)>$")
re_link = re.compile(r"^link<(\w+)>$")
re_dict = re.compile(r"^dict<((\w+\??:\w+,)*(\w+\??:\w+))>$")

# Dictionary containing validator functions for primitive data types
primitive_data_types = {
    "tag": bool,
    "binary": bool,
    "category": str,  # TODO
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

    Consists of additional attributes:
    - `data_type`: type for incoming value validation
    - `hashable`: whether contained data is hashable
    - `is_link`: whether this data type is link
    - `link_to`: if `is_link` is True, what is linked target
    """

    __root__: str
    _data_type = PrivateAttr(None)
    _hashable = PrivateAttr(True)
    _is_link = PrivateAttr(False)
    _link_to = PrivateAttr()

    def __init__(self, **data):
        super().__init__(**data)

        str_type = data["__root__"]

        self._hashable = not (
            "dict" in str_type
            or "set" in str_type
            or "array" in str_type
            or "special" in str_type
            or "json" in str_type
        )

        self.determine_value_validator(str_type)

    def determine_value_validator(self, str_type: str):
        """Determines value validator (inner `data_type`)

        This is not implemented inside @validator, because it apparently doesn't work with
        # __root__ models.
        """
        data_type = None

        if type(str_type) is not str:
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

        elif re.match(re_link, str_type):
            # Link
            target = str_type.split("<")[1].split(">")[0]
            data_type = str
            self._link_to = target
            self._is_link = True

        elif re.match(re_dict, str_type):
            # Dict
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
            data_type = create_model(f"{str_type}__inner", **dict_spec)
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

    def __str__(self):
        return self.__root__

    def __repr__(self):
        return f"'{str(self)}'"
