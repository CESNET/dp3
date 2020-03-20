import yaml
import ipaddress
import re

# Path to yaml file containing attribute specification
path_attr_spec = "C:/Users/bfu1/Desktop/ADiCT/AttrSpec.yml"

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
    for t in supported_data_types:
        if re.match(rf"^array<{t}>$", data_type):
            return True
    return False


# Check whether given data type represents a set
def is_set(data_type):
    for t in supported_data_types:
        if re.match(rf"^set<{t}>$", data_type):
            return True
    return False


# Check whether given data type represents a link
def is_link(data_type):
    for t in supported_data_types:
        if re.match(rf"^link<{t}>$", data_type):
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


# Validate array object
def valid_array(obj, validator):
    if type(obj) is not list:
        return False
    for item in obj:
        if not validator(item):
            return False
    return True


# Validate set object
def valid_set(obj, validator):
    if type(obj) is not list:
        return False
    for item in obj:
        if not validator(item) or obj.count(item) > 1:
            return False
    return True


# TODO Validate link object
def valid_link(obj):
    return False


# Load attribute specification (from yaml) and return it as a dict ('attr_id' -> AttrSpec)
# Raise TypeError or ValueError if the specification of some attribute is invalid
def load_spec():
    spec = yaml.safe_load(open(path_attr_spec))
    attr_spec = {}
    for attr in spec:
        attr_spec[attr] = AttrSpec(attr, spec[attr])

    return attr_spec


# This class represents specification of an attribute of given id
class AttrSpec:
    # Initialize member variables and validate attribute specification
    # Raise TypeError or ValueError if the specification is invalid (e.g. data type is not supported)
    def __init__(self, id, spec):
        self.id = id
        self.name = spec.get("name", id)
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

        # Dictionary containing validator functions for primitive data types
        validators = {
            "tag": lambda v: True,
            "binary": lambda v: v in {True, False, None},
            "category": lambda v: v in self.categories,
            "string": lambda v: type(v) is str,
            "int": lambda v: type(v) is int,
            "float": lambda v: type(v) is float,
            "ipv4": valid_ipv4,
            "ipv6": valid_ipv6,
            "special": lambda v: v is not None
        }

        # Initialize attribute's validator function according to its data type
        if self.data_type in supported_data_types:
            self.validator = validators[self.data_type]

        elif is_array(self.data_type):
            dtype = self.data_type.split("<")[1].split(">")[0]
            self.validator = lambda v: valid_array(v, validators[dtype])

        elif is_set(self.data_type):
            dtype = self.data_type.split("<")[1].split(">")[0]
            self.validator = lambda v: valid_set(v, validators[dtype])

        elif is_link(self.data_type):
            self.validator = lambda v: valid_link(v)

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
