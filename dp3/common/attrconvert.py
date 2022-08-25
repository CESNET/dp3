"""
Allows conversion of attributes from strings to their specified types.

TODO: consider adding `attr_type` (plain, observations, timeseries).
"""
import re
import json
from .attrspec import (
    re_array, re_dict, re_set, re_link,
    valid_ipv4, valid_ipv6, valid_mac
)
from dateutil.parser import parse as parsetime
from typing import Callable, Any

# Dictionary containing conversion functions for primitive data types
CONVERTERS = {
    "tag": lambda v: v,
    "binary": lambda v: True if v.lower() == 'true' else False,
    "string": str,
    "category": str,
    "int": int,
    "int64": int,
    "float": float,
    "ipv4": lambda v: _pass_valid(valid_ipv4, v),
    "ipv6": lambda v: _pass_valid(valid_ipv6, v),
    "mac": lambda v: _pass_valid(valid_mac, v),
    "time": parsetime,
    "json": json.loads,
}

def get_converter(attr_data_type:str) -> Callable[[str], Any]:
    """Return a function converting a string to given data type.

    TODO: Add `type` as parameter, not only `data_type`.
    """
    # empty type (typically timeseries)
    if not attr_data_type:
        return lambda v: None
    # basic type
    if attr_data_type in CONVERTERS:
        return CONVERTERS[attr_data_type]
    # array<X>
    m = re.match(re_array, attr_data_type)
    if m:
        return lambda x: _parse_array_str(x, m.group(1))
    # set<X>
    m = re.match(re_set, attr_data_type)
    if m:
        return lambda x: _parse_set_str(x, m.group(1))
    # dict<X,Y,Z>
    m = re.match(re_dict, attr_data_type)
    if m:
        # note: example dict spec format: dict<port:int,protocol:string,tag?:string>
        #       regex matches everything between <,> as group 1
        # dtype_mapping: dict_key -> data_type
        dtype_mapping = {key.rstrip("?"): dtype for key,dtype in (item.split(":") for item in m.group(1).split(','))}
        return lambda x: _parse_dict_str(x, dtype_mapping)
    # link<X>
    m = re.match(re_link, attr_data_type)
    if m:
        # TODO here we need to get data_type of ID of the linked entity_type
        # for now, assume it's string
        return lambda x: str(x)
    raise ValueError(f"No conversion function for attribute type '{attr_data_type}'")

def convert(attr_data_type:str, val:str) -> Any:
    """Convert given value to its specified type"""
    try:
        return get_converter(attr_data_type)(val)
    except ValueError:
        raise ValueError(f"Can't convert '{val}' to '{attr_data_type}'")

def get_element_type(attr_data_type:str) -> str:
    """Get data type of elements of given list or set attribute type"""
    if not is_iterable(attr_data_type):
        raise Exception(f'Given type is not iterable: {attr_data_type}')
    if re_dict.match(attr_data_type):
        return _get_dict_types(attr_data_type)
    element_type = attr_data_type.split("<")[1].split(">")[0]
    if element_type in CONVERTERS:
        return element_type
    raise Exception(f'Element type not configured in DB: {element_type}')

def is_iterable(attr_data_type:str) -> bool:
    if not attr_data_type:
        return False

    return bool(re.match(re_array, attr_data_type)
                or re.match(re_set, attr_data_type)
                or re.match(re_dict, attr_data_type))

def is_primitive_type(attr_data_type:str) -> bool:
    return attr_data_type in CONVERTERS

##########
# local helper functions

def _parse_array_str(string:str, item_type:str) -> list:
    """Parse string containing array of items of given type"""
    conv = CONVERTERS[item_type]
    a = json.loads(string)
    if not isinstance(a, list):
        raise ValueError
    return [conv(item) for item in a]

def _parse_set_str(string:str, item_type:str) -> set:
    conv = CONVERTERS[item_type]
    a = json.loads(string)
    if not isinstance(a, list):
        raise ValueError
    return set(conv(item) for item in a)

def _parse_dict_str(string:str, field_types:dict) -> dict:
    o = json.loads(string)
    if not isinstance(o, dict):
        raise ValueError
    return {k: CONVERTERS[field_types[k]](v) for k, v in o.items()}


def _pass_valid(validator_function, value):
    if validator_function(value):
        return value
    raise ValueError(f'The value {value} has invalid format.')


def _get_dict_types(attr_data_type: str) -> dict:
    dict_type_str = attr_data_type.split("<")[1].split(">")[0]
    dict_type_list = [item.split(":") for item in dict_type_str.split(",")]
    return dict((k[:-1] if k[-1] == '?' else k, v) for k, v in dict_type_list)
