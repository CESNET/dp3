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


# TODO Validate link object
def valid_link(obj, entity_type):
    # obj must be a valid key for given entity type
    return True


# Load attribute specification (from yaml) and return it as a dict ('attr_id' -> AttrSpec)
# Raise TypeError or ValueError if the specification of some attribute is invalid
def load_spec(path):
    spec = yaml.safe_load(open(path))
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

        # Set which operation to perform when queueing task for this attribute
        # TODO choose appropriate operation according to data type
        self.attr_update_op = "set"

        # Check data type of specification fields
        if type(self.name) is not str or \
           type(self.description) is not str or \
           type(self.color) is not str or not re.match(r"#([0-9a-fA-F]){6}", self.color) or \
           type(self.data_type) is not str or \
           type(self.timestamp) is not bool or \
           type(self.history) is not bool or \
           type(self.confidence) is not bool or \
           type(self.multi_value) is not bool or \
           type(self.timestamp_format) is not str:
            raise TypeError("Invalid specification of attribute '{}'".format(self.id))

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
        if self.data_type == "category" and type(self.categories) is not list:
            raise TypeError("Data type of 'categories' is invalid (should be list)")

        # If history is enabled, spec must contain a dict of history parameters
        if self.history is True:
            if type(self.history_params) is not dict:
                raise TypeError("Data type of 'history_params' is invalid (should be dict)")

            if "max_age" in self.history_params:
                if not re.match(r"(^0$|^[0-9]+[smhd]$)", str(self.history_params["max_age"])):
                    raise ValueError("Value of 'max_age' is invalid")
            else:
                self.history_params["max_age"] = default_max_age

            if "max_items" in self.history_params:
                if type(self.history_params["max_items"]) is not int or self.history_params["max_items"] <= 0:
                    raise ValueError("Value of 'max_items' is invalid")
            else:
                self.history_params["max_items"] = default_max_items

            if "expire_time" in self.history_params:
                if not re.match(r"(^0$|^inf$|^[0-9]+[smhd]$)", str(self.history_params["expire_time"])):
                    raise ValueError("Value of 'expire_time' is invalid")
            else:
                self.history_params["expire_time"] = default_expire_time

            # TODO other history params
