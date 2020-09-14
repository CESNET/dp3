"""
Allows conversion of attributes from strings to their specified types.
"""
import re
from .attrspec import (
    re_array, re_dict, re_set,
    valid_ipv4, valid_ipv6, valid_mac
)
from dateutil.parser import parse as parsetime


class AttrConvertor:
    @staticmethod
    def convert(attr_type, val):
        if attr_type in convertors:
            val = convertors[attr_type](val)
        elif re.match(re_array, attr_type):
            element_type = attr_type.split("<")[1].split(">")[0]
            val = convertors[element_type](val)
        else:
            raise ValueError(f'One does not simply convert a {attr_type}')
        return val

    @staticmethod
    def get_element_type(attr_type):
        if not AttrConvertor.is_iterable(attr_type):
            raise Exception(f'Given type is not iterable {attr_type}')
        if re_dict.match(attr_type):
            return "string"
        element_type = attr_type.split("<")[1].split(">")[0]
        if element_type in convertors:
            return element_type
        raise Exception(f'Element type not configured in DB {element_type}')

    @staticmethod
    def get_convertor(attr_type):
        return convertors[attr_type]

    @staticmethod
    def is_iterable(attr_type):
        return (re.match(re_array, attr_type)
                or re.match(re_set, attr_type)
                or re.match(re_dict, attr_type))

    @staticmethod
    def is_primitive_type(attr_type):
        return attr_type in convertors


# Dictionary containing conversion functions for primitive data types
convertors = {
    "tag": lambda v: v,
    "binary": lambda v: True if v.lower() == 'true' else False,
    "string": lambda v: str(v),
    "int": lambda v: int(v),
    "int64": lambda v: int(v),
    "float": lambda v: float(v),
    "ipv4": lambda v: pass_valid(valid_ipv4, v),
    "ipv6": lambda v: pass_valid(valid_ipv6, v),
    "mac": lambda v: pass_valid(valid_mac, v),
    "time": lambda v: parsetime(v),
    "special": lambda v: v, # deprecated, use json instead
    "json": lambda v: v
}


def pass_valid(validator_function, value):
    if validator_function(value):
        return value
    raise ValueError(f'The value {value} has invalid format.')
