import yaml
import ipaddress
import re

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
    "special"
]

# List of supported entity types
supported_entity_types = [
    "ip"
]

# Default specification fields
default_data_type = None
default_categories = None
default_history_params = None
default_timestamp_format = "%Y-%m-%d"  # %Y-%m-%dT%H:%M:%S.%f
default_color = "#000000"
default_description = ""
default_confidence = False
default_multi_value = False
default_timestamp = False
default_history = False
default_update_op = "set"

# Default history params
default_max_age = None
default_max_items = None
default_expire_time = "inf"


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
        if data_type.split("<")[1].split(">")[0] in supported_entity_types:
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


# Dictionary containing validator functions for primitive data types
validators = {
    "tag": lambda v: True,
    "binary": lambda v: v in {True, False},
    "string": lambda v: type(v) is str,
    "int": lambda v: type(v) is int,
    "float": lambda v: type(v) is float,
    "ipv4": valid_ipv4,
    "ipv6": valid_ipv6,
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
    # obj must be a valid key for given entity type
    # TODO add other entity types
    return valid_ipv4(obj)


# Load attribute specification (from yaml) and return it as a dict ('attr_id' -> AttrSpec)
# Raise TypeError or ValueError if the specification of some attribute is invalid
def load_spec(path):
    spec = yaml.safe_load(open(path, "r"))
    attr_spec = {}
    for attr in spec:
        attr_spec[attr] = AttrSpec(attr, spec[attr])

    return attr_spec


# This class represents specification of an attribute of given id
class AttrSpec:
    # Initialize member variables and validate attribute specification
    # Raise TypeError or ValueError if the specification is invalid (e.g. data type is not supported)
    def __init__(self, attr_id, spec):
        self.id = attr_id
        self.name = spec.get("name", attr_id)
        self.description = spec.get("description", default_description)
        self.color = spec.get("color", default_color)
        self.data_type = spec.get("data_type", default_data_type)
        self.categories = spec.get("categories", default_categories)
        self.timestamp = spec.get("timestamp", default_timestamp)
        self.history = spec.get("history", default_history)
        self.confidence = spec.get("confidence", default_confidence)
        self.multi_value = spec.get("multi_value", default_multi_value)
        self.history_params = spec.get("history_params", default_history_params)
        self.timestamp_format = spec.get("timestamp_format", default_timestamp_format)
        self.attr_update_op = spec.get("attr_update_op", default_update_op)

        # Check data type of specification fields
        assert type(self.name) is str, "Type of 'name' is not 'str'"
        assert type(self.description) is str, "Type of 'description' is not 'str'"
        assert type(self.color) is str, "Type of 'color' is not 'str'"
        assert type(self.data_type) is str, "Type of 'data_type' is not 'str'"
        assert type(self.timestamp) is bool, "Type of 'timestamp' is not 'bool'"
        assert type(self.history) is bool, "Type of 'history' is not 'bool'"
        assert type(self.confidence) is bool, "Type of 'confidence' is not 'bool'"
        assert type(self.multi_value) is bool, "Type of 'multi_value' is not 'bool'"
        assert type(self.timestamp_format) is str, "Type of 'timestamp_format' is not 'str'"
        assert type(self.attr_update_op) is str, "Type of 'attr_update_op' is not 'str'"

        # Check color format
        assert re.match(r"#([0-9a-fA-F]){6}", self.color), "Format of 'color' is invalid"

        # Initialize attribute's validator function according to its data type
        if self.data_type == "category":
            self.validator = lambda v: v in self.categories

        elif self.data_type in supported_data_types:
            self.validator = validators[self.data_type]

        elif is_array(self.data_type):
            dtype = self.data_type.split("<")[1].split(">")[0]
            self.validator = lambda v: valid_array(v, dtype)

        elif is_set(self.data_type):
            dtype = self.data_type.split("<")[1].split(">")[0]
            self.validator = lambda v: valid_set(v, dtype)

        elif is_link(self.data_type):
            etype = self.data_type.split("<")[1].split(">")[0]
            self.validator = lambda v: valid_link(v, etype)

        else:
            raise ValueError("Type '{}' is not supported".format(self.data_type))

        # If value is of category type, spec must contain a list of valid categories
        assert self.data_type != "category" or type(self.categories) is list, "Type of 'categories' is invalid (must be list)"

        # If history is enabled, spec must contain a dict of history parameters
        if self.history is True:
            assert type(self.history_params) is dict, "Type of 'history_params' is invalid (must be dict)"

            if "max_age" in self.history_params:
                assert re.match(r"(^0$|^[0-9]+[smhd]$)", str(self.history_params["max_age"])), "Format of 'max_age' is invalid"
            else:
                self.history_params["max_age"] = default_max_age

            if "max_items" in self.history_params:
                assert type(self.history_params["max_items"]) is int, "Type of 'max_items' is invalid (must be int)"
                assert self.history_params["max_items"] > 0, "Value of 'max_items' is invalid (must be greater than 0)"
            else:
                self.history_params["max_items"] = default_max_items

            if "expire_time" in self.history_params:
                assert re.match(r"(^0$|^inf$|^[0-9]+[smhd]$)", str(self.history_params["expire_time"])), "Format of 'expire_time' is invalid"
            else:
                self.history_params["expire_time"] = default_expire_time