import time

# Error message templates
err_msg_type = "type of '{}' is invalid (must be '{}')"
err_msg_format = "format of '{}' is invalid"
err_msg_value = "value of '{}' is invalid"

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
    # Check mandatory fields and their types
    assert type(record["type"]) is str, err_msg_type.format("type", "str")
    assert type(record["id"]) is str, err_msg_type.format("id", "str")
    assert type(record["attr"]) is str, err_msg_type.format("attr", "str")
    
    # Check whether 'attr_spec' contains the attribute
    assert record["attr"] in attr_spec, f"No specification found for attribute '{record['attr']}'"

    spec = attr_spec[record["attr"]]

    if spec.timestamp is True:
        # Try parsing timestamp values
        t1 = time.strptime(record["t1"], spec.timestamp_format)
        t2 = time.strptime(record["t2"], spec.timestamp_format)

        # Check valid time interval (T2 must be greater than or equal to T1)
        assert t1 <= t2, err_msg_value.format("t2")

    if spec.confidence is True:
        # Confidence must be a valid float on interval [0,1]
        assert type(record["c"]) is float, err_msg_type.format("c", "float")
        assert record["c"] >= 0 and record["c"] <= 1, err_msg_value.format("c")

    # Check data type of value field - validator functions are specified in AttrSpec module
    assert spec.validator(record["v"]), err_msg_value.format("v")


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
