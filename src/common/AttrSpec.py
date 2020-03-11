import yaml
import time
import ipaddress

# Default specification fields
default_data_type = None
default_categories = None
default_history_params = None
default_timestamp_format = "%Y-%m-%d"  # %Y-%m-%dT%H:%M:%S.%f
default_color = "#000000"
default_description = ""
default_confidence = False
default_multi_value = False
default_timestamp = True
default_history = False
default_max_age = "30d"
default_max_items = 1000

# Default record fields
default_type = None
default_id = None
default_attr = None
default_v = None
default_t1 = None
default_c = 1
default_src = "Unknown"

# List of supported data types
supported_data_types = {
    "tag",
    "binary",
    "category",
    "string",
    "int",
    "float",
    "ipv4",
    "ipv6",
    "special"
    # TODO:
    # Array, set and link types
    # Maybe use multi_value to indicate that value field is an array/list? If so, what about set?
    # How to represent link?
}


# Dictionary containing attribute specifications ('attr_id' -> AttrSpec)
# Must be initialized by load_yaml()
attr_spec = {}


# Initialize 'attr_spec' dictionary from yaml config file
def load_yaml(yaml_path):
    d = yaml.safe_load(open(yaml_path))
    for k in d:
        attr_spec[k] = AttrSpec(k, d[k])


# Validate ipv4 string
def valid_ipv4(address):
    try:
        ipaddress.IPv4Address(address)
        return True
    except:
        return False


# Validate ipv6 string
def valid_ipv6(address):
    try:
        ipaddress.IPv6Address(address)
        return True
    except:
        return False


# Validate record fields according to attribute specification
def validate_record(record):
    # Check mandatory fields
    if type(record["type"]) is not str or \
       type(record["id"]) is not str or \
       type(record["attr"]) is not str:
        raise TypeError

    # Check whether 'attr_spec' contains the attribute
    if record["attr"] not in attr_spec:
        raise ValueError

    spec = attr_spec[record["attr"]]

    if spec.timestamp is True:
        try:
            # Try parsing timestamp values
            t1 = time.strptime(record["t1"], spec.timestamp_format)
            t2 = time.strptime(record["t2"], spec.timestamp_format)

            # Check valid time interval (T2 must be greater than or equal to T1)
            if t2 < t1:
                raise ValueError
        except:
            raise

    if spec.confidence is True:
        if type(record["c"]) is not float:
            raise TypeError
        if record["c"] < 0 or record["c"] > 1:
            raise ValueError

    # Dictionary containing validator functions for supported data types
    validators = {
        "tag": lambda v: True,
        "binary": lambda v: v in {True, False, None},
        "category": lambda v: v in spec.categories,
        "string": lambda v: type(v) is str,
        "int": lambda v: type(v) is int,
        "float": lambda v: type(v) is float,
        "ipv4": valid_ipv4,
        "ipv6": valid_ipv6,
        "special": lambda v: v is not None
    }

    # Check data type of value field
    f = validators.get(spec.data_type, lambda v: False)
    if not f(record["v"]):
        raise TypeError


# Create a record (from JSON/dict) according to attribute specification
def make_record(r):
    # Set missing fields to default values
    record = {
        "type": r.get("type", default_type),
        "id": r.get("id", default_id),
        "attr": r.get("attr", default_attr),
        "t1": r.get("t1", default_t1),
        "t2": r.get("t2", r.get("t1", default_t1)),
        "v": r.get("v", default_v),
        "c": r.get("c", default_c),
        "src": r.get("src", default_src)
    }

    try:
        # Validate the record
        validate_record(record)
    except:
        raise
    return record


# This class represents specification of an attribute of given id
class AttrSpec:
    # Validate attribute specification
    def valid_spec(self):
        # Data type must be supported
        if self.data_type not in supported_data_types:
            return False

        # If value is of category type, spec must contain a list of valid categories
        if self.data_type == "category" and type(self.categories) is not list:
            return False

        # If history is enabled, spec must contain a dict of history parameters
        if self.history is True:
            if type(self.history_params) is not dict:
                return False
            if "max_age" not in self.history_params:
                self.history_params["max_age"] = default_max_age
            if "max_items" not in self.history_params:
                self.history_params["max_items"] = default_max_items
            # TODO other history params
        return True

    # Initialize member variables and validate attribute specification
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

        if not self.valid_spec():
            raise ValueError
