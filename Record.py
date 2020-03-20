import time

# Default record fields
default_type = None
default_id = None
default_attr = None
default_v = None
default_t1 = None
default_c = 1
default_src = ""


# Validate record fields according to given attribute specification
def validate_record(record, attr_spec):
    # Check mandatory fields
    if type(record["type"]) is not str or \
       type(record["id"]) is not str or \
       type(record["attr"]) is not str:
        raise TypeError("Data type of mandatory field(s) is invalid")

    # Check whether 'attr_spec' contains the attribute
    if record["attr"] not in attr_spec:
        raise ValueError("No specification found for attribute '{}'".format(record["attr"]))

    spec = attr_spec[record["attr"]]

    if spec.timestamp is True:
        # Try parsing timestamp values
        t1 = time.strptime(record["t1"], spec.timestamp_format)
        t2 = time.strptime(record["t2"], spec.timestamp_format)

        # Check valid time interval (T2 must be greater than or equal to T1)
        if t2 < t1:
            raise ValueError("Time interval is invalid (t2 < t1)")

    if spec.confidence is True:
        # Confidence must be a valid float on interval [0,1]
        if type(record["c"]) is not float:
            raise TypeError("Type of 'confidence' is not 'float'")
        if record["c"] < 0 or record["c"] > 1:
            raise ValueError("Value of 'confidence' is invalid")

    # Check 'value' field of the record (value must match its data type)
    # Validator functions for supported data types are specified in AttrSpec module
    if not spec.validator(record["v"]):
        raise ValueError("Validation of 'value' failed.")


class Record:
    # Create a record (from JSON/dict) according to attribute specification
    # Raise ValueError if the value of some field is invalid
    # Raise TypeError if the data type of some field is invalid
    def __init__(self, r, spec):
        # Set missing fields to default values
        self.d = {
            "type": r.get("type", default_type),
            "id": r.get("id", default_id),
            "attr": r.get("attr", default_attr),
            "t1": r.get("t1", default_t1),
            "t2": r.get("t2", r.get("t1", default_t1)),
            "v": r.get("v", default_v),
            "c": r.get("c", default_c),
            "src": r.get("src", default_src)
        }

        # Validate the record
        validate_record(self, spec)

    def __getitem__(self, item):
        return self.d[item]
