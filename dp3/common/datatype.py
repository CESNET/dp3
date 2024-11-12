import re
from datetime import datetime
from enum import Enum
from ipaddress import IPv4Address, IPv6Address
from typing import Any, Optional, Union

from pydantic import (
    BaseModel,
    Json,
    PrivateAttr,
    RootModel,
    create_model,
    model_validator,
)

from dp3.common.context import get_entity_context
from dp3.common.mac_address import MACAddress

# Regular expressions for parsing various data types
re_array = re.compile(r"^array<(.+)>$")
re_set = re.compile(r"^set<(.+)>$")
re_link = re.compile(
    r"^link\s*<\s*(?P<etype>\w+)\s*(?:,\s*(?P<data>\S+)\s*)?(?:;\s*mirror=(?P<mirror>\S+)\s*)?>$"
)
re_dict = re.compile(r"^dict\s*<(\s*(\w+\??:\w+,\s*)*(\w+\??:\w+\s*))>$")
re_category = re.compile(r"^category<\s*(?P<type>\w+)\s*;\s*(?P<vals>(?:\s*\w+,\s*)*\s*\w+\s*)>$")

# Dictionary containing validator functions for primitive data types
primitive_data_types = {
    "tag": bool,
    "binary": bool,
    "string": str,
    "int": int,
    "int64": int,
    "float": float,
    "ipv4": IPv4Address,
    "ipv6": IPv6Address,
    "mac": MACAddress,
    "time": datetime,
    "special": Any,
    "json": Union[Json[Any], dict, list],
}

eid_data_types = ["string", "int", "ipv4", "ipv6", "mac"]

AnyEidT = Union[str, int, IPv4Address, IPv6Address, MACAddress]
"""Type alias for any of possible entity ID data types.

Note that the type is determined based on the loaded entity configuration
and in most cases is only one of the options, based on what entity is being processed.
"""


class ReadOnly(BaseModel):
    """The ReadOnly data_type is used to avoid datapoint insertion for an attribute."""

    @model_validator(mode="before")
    @classmethod
    def fail(cls, data):
        raise ValueError("Cannot instantiate a read-only attribute datapoint.")


class Link(BaseModel, extra="forbid"):
    eid: Any


class DataType(RootModel):
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
    """

    root: str
    _data_type = PrivateAttr(None)
    _type_info = PrivateAttr(None)
    _hashable = PrivateAttr(True)

    _iterable = PrivateAttr(False)
    _elem_type = PrivateAttr()

    _is_link = PrivateAttr(False)
    _link_to = PrivateAttr()
    _mirror_link = PrivateAttr(False)
    _mirror_as = PrivateAttr()
    _link_data = PrivateAttr()

    def _determine_value_validator(self):
        """Determines value validator (inner `data_type`)."""
        str_type = self.root

        self._hashable = not (
            "dict" in str_type
            or "set" in str_type
            or "array" in str_type
            or "special" in str_type
            or "json" in str_type
            or "link" in str_type
        )

        if str_type in primitive_data_types:
            # Primitive type
            data_type = primitive_data_types[str_type]

        elif m := re.match(re_array, str_type):
            # Array
            element_type = m.group(1).strip()
            value_type = DataType(root=element_type)
            if not is_primitive_element_type(value_type):
                raise ValueError(f"Data type {element_type} is not supported as an array element")
            data_type = list[value_type.data_type]
            self._iterable = True
            self._elem_type = value_type

        elif m := re.match(re_set, str_type):
            # Set
            element_type = m.group(1).strip()
            value_type = DataType(root=element_type)
            if not is_primitive_element_type(value_type):
                raise ValueError(f"Data type {element_type} is not supported as a set element")
            data_type = list[value_type.data_type]  # set is not supported by MongoDB
            self._iterable = True
            self._elem_type = value_type

        elif m := re.match(re_link, str_type):
            # Link
            etype, data, mirrored = m.group("etype"), m.group("data"), m.group("mirror")
            self._link_to = etype
            self._is_link = True
            self._link_data = bool(data)
            self._mirror_link = bool(mirrored)
            self._mirror_as = mirrored if mirrored else None
            self._type_info = f"link<{etype},{data}>"

            context = get_entity_context()
            if etype not in context["entities"]:
                raise ValueError(f"Entity type '{etype}' is not defined")
            entity_spec = context["entities"][etype]

            if etype and data:
                value_type = DataType(root=data)
                data_type = create_model(
                    f"Link<{data}>",
                    __base__=Link,
                    eid=(entity_spec.id_data_type.data_type, ...),
                    data=(value_type.data_type, ...),
                )
            else:
                data_type = create_model(
                    f"Link<{etype}>",
                    __base__=Link,
                    eid=(entity_spec.id_data_type.data_type, ...),
                )

        elif re.match(re_dict, str_type):
            # Dict
            dict_spec = {}

            key_str = str_type.split("<")[1].split(">")[0]
            key_spec = dict(item.strip().split(":") for item in key_str.split(","))

            # For each dict key
            for k, v in key_spec.items():
                if v not in primitive_data_types:
                    raise ValueError(f"Data type {v} of key {k} is not supported as a dict field")

                # Optional subattribute
                k_optional = k[-1] == "?"

                # Set (type, default value) for the key
                if k_optional:
                    k = k[:-1]  # Remove question mark from key
                    dict_spec[k] = (Optional[primitive_data_types[v]], None)
                else:
                    dict_spec[k] = (primitive_data_types[v], ...)

            # Create model for this dict
            data_type = create_model(f"{str_type}__inner", **dict_spec)
            self._type_info = (
                "dict<" + ",".join(f"{k}:{v}" for k, v in sorted(dict_spec.items())) + ">"
            )

        elif m := re.match(re_category, str_type):
            # Category
            category_type, category_values = m.group("type"), m.group("vals")

            category_type = DataType(root=category_type)
            category_values = [
                category_type._data_type(value.strip()) for value in category_values.split(",")
            ]

            data_type = Enum(
                f"Category<{category_type}>", {str(val): val for val in category_values}
            )
        else:
            raise ValueError(f"Data type '{str_type}' is not supported")

        # Set data type
        self._data_type = data_type
        # Set default type info
        if self._type_info is None:
            self._type_info = str(data_type)
        return self

    @model_validator(mode="after")
    def determine_value_validator(self):
        """Determines value validator (inner `data_type`)."""
        return self._determine_value_validator()

    @property
    def data_type(self) -> Union[type, BaseModel]:
        """Type for incoming value validation"""
        return self._data_type

    @property
    def type_info(self) -> str:
        """String representation of the data type, immune to whitespace changes"""
        return self._type_info

    @property
    def hashable(self) -> bool:
        """Whether contained data is hashable"""
        return self._hashable

    @property
    def iterable(self) -> bool:
        """Whether the data type is iterable"""
        return self._iterable

    @property
    def elem_type(self) -> "DataType":
        """if `iterable`, the element data type"""
        return self._elem_type

    @property
    def is_link(self) -> bool:
        """Whether the data type is a link between entities"""
        return self._is_link

    @property
    def mirror_link(self) -> bool:
        """If `is_link`, whether the link is mirrored"""
        return self._mirror_link

    @property
    def mirror_as(self) -> Union[str, None]:
        """If `mirror_link`, what is the name of the mirrored attribute"""
        return self._mirror_as

    @property
    def link_to(self) -> str:
        """If `is_link`, the target linked entity"""
        return self._link_to

    def get_linked_entity(self) -> str:
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
        return self.root

    def __repr__(self):
        return f"'{str(self)}'"


class EidDataType(DataType):
    """Data type container for entity id

    Represents one of primitive data types:
    - string
    - int
    - ipv4
    - ipv6
    - mac
    """

    @model_validator(mode="after")
    def determine_value_validator(self):
        if self.root not in eid_data_types:
            raise ValueError(f"Data type of entity ID must be one of {eid_data_types}")
        return self._determine_value_validator()


def is_primitive_element_type(data_type: DataType) -> bool:
    return data_type.root in primitive_data_types or data_type.is_link
