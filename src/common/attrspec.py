import ipaddress
import re
import time

# Error message templates
err_msg_type = "type of '{}' is invalid (must be '{}')"
err_msg_format = "format of '{}' is invalid"
err_msg_value = "value of '{}' is invalid"
err_msg_missing_field = "mandatory field '{}' is missing"

# List of primitive data types
supported_data_types = [
    "tag",
    "binary",
    "category",
    "string",
    "int",
    "float",
    "ipv4",
    "ipv6",
    "time",
    "special"
]

# Default specification fields
default_color = "#000000"
default_description = ""
default_confidence = False
default_multi_value = False
default_timestamp = False
default_history = False

# Default history params
default_max_age = None
default_max_items = None
default_expire_time = "inf"

# Regular expression for parsing RFC3339 time format (with optional fractional part and timezone) 
timestamp_re = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt ][0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?([Zz]|(?:[+-][0-9]{2}:[0-9]{2}))?$")

# Check whether given data type represents an array
def is_array(data_type):
    if re.match(r"^array<\w+>$", data_type):
        if data_type.split("<")[1].split(">")[0] in supported_data_types:
            return True
    return False


# Check whether given data type represents a set
def is_set(data_type):
    if re.match(r"^set<\w+>$", data_type):
        if data_type.split("<")[1].split(">")[0] in supported_data_types:
            return True
    return False


# Check whether given data type represents a link
def is_link(data_type):
    if re.match(r"^link<\w+>$", data_type):
        # TODO
        # if data_type.split("<")[1].split(">")[0] in supported_entity_types:
            return True
    return False


# Validate ipv4 string
def valid_ipv4(address):
    try:
        ipaddress.IPv4Address(address)
        return True
    except ValueError:
        return False


# Validate ipv6 string
def valid_ipv6(address):
    try:
        ipaddress.IPv6Address(address)
        return True
    except ValueError:
        return False


# Validate timestamp string
def valid_rfc3339(timestamp):
    if timestamp_re.match(timestamp):
        return True
    else:
        return False


# Dictionary containing validator functions for primitive data types
validators = {
    "tag": lambda v: True,
    "binary": lambda v: v in {True, False},
    "string": lambda v: type(v) is str,
    "int": lambda v: type(v) is int,
    "float": lambda v: type(v) is float,
    "ipv4": valid_ipv4,
    "ipv6": valid_ipv6,
    "time": valid_rfc3339,
    "special": lambda v: v is not None
}


# Validate array object
def valid_array(obj, data_type):
    if type(obj) is not list:
        return False
    f = validators[data_type]
    for item in obj:
        if not f(item):
            return False
    return True


# Validate set object
def valid_set(obj, data_type):
    if type(obj) is not list:
        return False
    f = validators[data_type]
    for item in obj:
        if not f(item) or obj.count(item) > 1:
            return False
    return True


def valid_link(obj, entity_type):
    # TODO
    # obj must be a valid key for given entity type
    return True


# This class represents specification of an attribute of given id
class AttrSpec:
    # Class constructor
    # Raises AssertionError if the specification is invalid
    def __init__(self, id, spec):
        # Set default values for missing fields
        self.id = id
        self.name = spec.get("name", self.id)
        self.description = spec.get("description", default_description)
        self.color = spec.get("color", default_color)
        self.data_type = spec.get("data_type", None)
        self.categories = spec.get("categories", None)
        self.timestamp = spec.get("timestamp", default_timestamp)
        self.history = spec.get("history", default_history)
        self.confidence = spec.get("confidence", default_confidence)
        self.multi_value = spec.get("multi_value", default_multi_value)
        self.history_params = spec.get("history_params", None)

        # Check mandatory specification fields
        assert self.id is not None, err_msg_missing_field.format("id")
        assert self.data_type is not None, err_msg_missing_field.format("data_type")

        # Check data type of specification fields
        assert type(self.id) is str, err_msg_type.format("id", "str")
        assert type(self.name) is str, err_msg_type.format("name", "str")
        assert type(self.description) is str, err_msg_type.format("description", "str")
        assert type(self.color) is str, err_msg_type.format("color", "str")
        assert type(self.data_type) is str, err_msg_type.format("data_type", "str")
        assert type(self.timestamp) is bool, err_msg_type.format("timestamp", "bool")
        assert type(self.history) is bool, err_msg_type.format("history", "bool")
        assert type(self.confidence) is bool, err_msg_type.format("confidence", "bool")
        assert type(self.multi_value) is bool, err_msg_type.format("multi_value", "bool")

        # Check color format
        assert re.match(r"#([0-9a-fA-F]){6}", self.color), err_msg_format.format("color")

        # Initialize attribute's validator function according to its data type
        if self.data_type == "category":
            if self.categories is None:
                self.value_validator = validators["string"]
            else:
                self.value_validator = lambda v: v in self.categories

        elif self.data_type in supported_data_types:
            self.value_validator = validators[self.data_type]

        elif is_array(self.data_type):
            dtype = self.data_type.split("<")[1].split(">")[0]
            self.value_validator = lambda v: valid_array(v, dtype)

        elif is_set(self.data_type):
            dtype = self.data_type.split("<")[1].split(">")[0]
            self.value_validator = lambda v: valid_set(v, dtype)

        elif is_link(self.data_type):
            etype = self.data_type.split("<")[1].split(">")[0]
            self.value_validator = lambda v: valid_link(v, etype)

        else:
            raise AssertionError("type '{}' is not supported".format(self.data_type))

        # If categories are given, it should be a list
        if self.categories:
            assert type(self.categories) is list, err_msg_type.format("categories", "list")

        # If history is enabled, spec must contain a dict of history parameters
        if self.history is True:
            assert self.history_params is not None, err_msg_missing_field.format("history_params")
            assert type(self.history_params) is dict, err_msg_type.format("history_params", "dict")

            if "max_age" in self.history_params:
                assert re.match(r"(^0$|^[0-9]+[smhd]$)", str(self.history_params["max_age"])), err_msg_format.format("max_age")
            else:
                self.history_params["max_age"] = default_max_age

            if "max_items" in self.history_params:
                assert type(self.history_params["max_items"]) is int, err_msg_type.format("max_items", "int")
                assert self.history_params["max_items"] > 0, err_msg_value.format("max_items")
            else:
                self.history_params["max_items"] = default_max_items

            if "expire_time" in self.history_params:
                assert re.match(r"(^0$|^inf$|^[0-9]+[smhd]$)", str(self.history_params["expire_time"])), err_msg_format.format("expire_time")
            else:
                self.history_params["expire_time"] = default_expire_time
