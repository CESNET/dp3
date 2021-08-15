import ipaddress
import re
from dp3.common.utils import parse_time_duration

# Error message templates
err_msg_type = "type of '{}' is invalid (must be '{}')"
err_msg_format = "format of '{}' is invalid"
err_msg_value = "value of '{}' is invalid"
err_msg_missing_field = "mandatory field '{}' is missing"

# List of primitive data types
primitive_data_types = [
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
    "special", # deprecated, use json instead
    "json"
]

# List of aggregation functions
aggregation_functions = [
    "keep",
    "add",
    "avg",
    "min",
    "max",
    "csv_union"
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
default_pre_validity = "0s"
default_post_validity = "0s"
default_aggregation_interval = "0s"
default_aggregation_max_age = "0s"
default_aggregation_function_value = "keep"
default_aggregation_function_confidence = "avg"
default_aggregation_function_source = "csv_union"

# Regular expressions for parsing various data types
re_timestamp = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt ][0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?([Zz]|(?:[+-][0-9]{2}:[0-9]{2}))?$")
re_mac = re.compile(r"^([0-9a-fA-F]{2}[:-]){5}([0-9a-fA-F]{2})$")
re_array = re.compile(r"^array<(\w+)>$")
re_set = re.compile(r"^set<(\w+)>$")
re_link = re.compile(r"^link<(\w+)>$")
re_dict = re.compile(r"^dict<((\w+\??:\w+,)*(\w+\??:\w+))>$")


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
    return re_timestamp.match(timestamp)


# Validate MAC string
def valid_mac(address):
    return re_mac.match(address)


# Dictionary containing validator functions for primitive data types
validators = {
    "tag": lambda v: type(v) is bool,
    "binary": lambda v: type(v) is bool,
    "string": lambda v: type(v) is str,
    "int": lambda v: type(v) is int,
    "int64": lambda v: type(v) is int,
    "float": lambda v: type(v) is float,
    "ipv4": valid_ipv4,
    "ipv6": valid_ipv6,
    "mac": valid_mac,
    "time": valid_rfc3339,
    "special": lambda v: v is not None,
    "json": lambda v: v is not None  # TODO validate json format?
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


# Validate dict object
def valid_dict(obj, key_spec):
    if type(obj) is not dict:
        return False
    for key in key_spec:
        if key not in obj:
            if key[-1] == "?":
                continue
            else:
                return False
        f = validators[key_spec[key]]
        if not f(obj[key]):
            return False
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
        if self.data_type in primitive_data_types:
            self.value_validator = validators[self.data_type]

        elif self.data_type == "category":
            if self.categories is None:
                self.value_validator = validators["string"]
            else:
                assert type(self.categories) is list, err_msg_type.format("categories", "list")
                self.value_validator = lambda v: v in self.categories

        elif re.match(re_array, self.data_type):
            element_type = self.data_type.split("<")[1].split(">")[0]
            assert element_type in primitive_data_types, f"data type {element_type} is not supported as an array element"
            self.value_validator = lambda v: valid_array(v, element_type)

        elif re.match(re_set, self.data_type):
            element_type = self.data_type.split("<")[1].split(">")[0]
            assert element_type in primitive_data_types, f"data type {element_type} is not supported as a set element"
            self.value_validator = lambda v: valid_set(v, element_type)

        elif re.match(re_link, self.data_type):
            # TODO
            # Should the entity type be validated here? I.e. does the specification for given entity type have to exist?
            self.value_validator = lambda v: v is not None
        
        elif re.match(re_dict, self.data_type):
            key_str = self.data_type.split("<")[1].split(">")[0]
            key_spec = dict(item.split(":") for item in key_str.split(","))
            for k in key_spec:
                assert key_spec[k] in primitive_data_types, f"data type {key_spec[k]} is not supported as a dict field"
            self.value_validator = lambda v: valid_dict(v, key_spec)

        else:
            raise AssertionError(f"data type '{self.data_type}' is not supported")

        # If history is enabled, spec must contain a dict of history parameters
        if self.history is True:
            assert self.history_params is not None, err_msg_missing_field.format("history_params")
            assert type(self.history_params) is dict, err_msg_type.format("history_params", "dict")

            if "max_items" in self.history_params:
                assert type(self.history_params["max_items"]) is int, err_msg_type.format("max_items", "int")
                assert self.history_params["max_items"] > 0, err_msg_value.format("max_items")
            else:
                self.history_params["max_items"] = default_max_items

            # TODO simplify
            if "max_age" in self.history_params:
                assert re.match(r"(^0$|^[0-9]+[smhd]$)", str(self.history_params["max_age"])), err_msg_format.format("max_age")
            else:
                self.history_params["max_age"] = default_max_age
            self.history_params["max_age"] = parse_time_duration(self.history_params["max_age"])

            if "expire_time" in self.history_params:
                assert re.match(r"(^0$|^inf$|^[0-9]+[smhd]$)", str(self.history_params["expire_time"])), err_msg_format.format("expire_time")
            else:
                self.history_params["expire_time"] = default_expire_time
            self.history_params["expire_time"] = parse_time_duration(self.history_params["expire_time"])

            if "pre_validity" in self.history_params:
                assert re.match(r"(^0$|^[0-9]+[smhd]$)", str(self.history_params["pre_validity"])), err_msg_format.format("pre_validity")
            else:
                self.history_params["pre_validity"] = default_pre_validity
            self.history_params["pre_validity"] = parse_time_duration(self.history_params["pre_validity"])

            if "post_validity" in self.history_params:
                assert re.match(r"(^0$|^[0-9]+[smhd]$)", str(self.history_params["post_validity"])), err_msg_format.format("post_validity")
            else:
                self.history_params["post_validity"] = default_post_validity
            self.history_params["post_validity"] = parse_time_duration(self.history_params["post_validity"])

            if "aggregation_interval" in self.history_params:
                assert re.match(r"(^0$|^[0-9]+[smhd]$)", str(self.history_params["aggregation_interval"])), err_msg_format.format("aggregation_interval")
            else:
                self.history_params["aggregation_interval"] = default_aggregation_interval
            self.history_params["aggregation_interval"] = parse_time_duration(self.history_params["aggregation_interval"])

            if "aggregation_max_age" in self.history_params:
                assert re.match(r"(^0$|^[0-9]+[smhd]$)", str(self.history_params["aggregation_max_age"])), err_msg_format.format("aggregation_max_age")
            else:
                self.history_params["aggregation_max_age"] = default_aggregation_max_age
            self.history_params["aggregation_max_age"] = parse_time_duration(self.history_params["aggregation_max_age"])

            if "aggregation_function_value" in self.history_params:
                assert self.history_params["aggregation_function_value"] in aggregation_functions, err_msg_format.format("aggregation_function_value")
            else:
                self.history_params["aggregation_function_value"] = default_aggregation_function_value

            if "aggregation_function_confidence" in self.history_params:
                assert self.history_params["aggregation_function_confidence"] in aggregation_functions, err_msg_format.format("aggregation_function_confidence")
            else:
                self.history_params["aggregation_function_confidence"] = default_aggregation_function_confidence

            if "aggregation_function_source" in self.history_params:
                assert self.history_params["aggregation_function_source"] in aggregation_functions, err_msg_format.format("aggregation_function_source")
            else:
                self.history_params["aggregation_function_source"] = default_aggregation_function_source

    def __repr__(self):
        """Return string whose evaluation would create the same object."""
        attrs = {'name': self.name}
        if self.description:
           attrs['description'] = self.description
        if self.color != default_color:
            attrs['color'] = self.color
        attrs['data_type'] = self.data_type
        if self.categories:
            attrs['categories'] = self.categories
        attrs['confidence'] = self.confidence
        attrs['multi_value'] = self.multi_value
        attrs['history'] = self.history
        if self.history_params:
            attrs['history_params'] = self.history_params
        return f"AttrSpec({self.id!r}, {attrs!r})"

    # TODO shorter and more readable __str__ representation?
