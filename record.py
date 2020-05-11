import time

# Error message templates
err_msg_type = "type of '{}' is invalid (must be '{}')"
err_msg_format = "format of '{}' is invalid"
err_msg_value = "value of '{}' is invalid"
err_msg_missing_field = "mandatory field '{}' is missing"

# Default record fields
default_v = None
default_t1 = None
default_c = 1
default_src = ""

# Required timestamp format
# TODO
timestamp_format = ""


# Validate record fields according to given entity/attribute specification
def validate_record(record, config):
    # Check mandatory fields
    assert record["type"] is not None, err_msg_missing_field.format("type")
    assert record["id"] is not None, err_msg_missing_field.format("id")
    assert record["attr"] is not None, err_msg_missing_field.format("attr")

    # Check data type of mandatory fields
    assert type(record["type"]) is str, err_msg_type.format("type", "str")
    assert type(record["id"]) is str, err_msg_type.format("id", "str")
    assert type(record["attr"]) is str, err_msg_type.format("attr", "str")

    # Check whether 'attr_spec' contains the entity/attribute
    assert record["type"] in config, f"No specification found for entity type '{record['type']}'"
    assert record["attr"] in config[record["type"]]["attribs"], f"No specification found for attribute '{record['attr']}'"
    attr_spec = config[record["type"]]["attribs"][record["attr"]]

    if attr_spec.timestamp is True:
        # Try parsing timestamp values
        t1 = time.strptime(record["t1"], timestamp_format)
        t2 = time.strptime(record["t2"], timestamp_format)

        # Check valid time interval (T2 must be greater than or equal to T1)
        assert t1 <= t2, err_msg_value.format("t2")

    if attr_spec.confidence is True:
        # Confidence must be a valid float on interval [0,1]
        assert type(record["c"]) is float, err_msg_type.format("c", "float")
        assert record["c"] >= 0 and record["c"] <= 1, err_msg_value.format("c")

    # Check data type of value field - validator functions are specified in AttrSpec module
    assert attr_spec.value_validator(record["v"]), err_msg_type.format("v", attr_spec.data_type)


class Record:
    # Create a record (from JSON/dict) according to entity/attribute specification
    # Raises an error if the record is invalid
    def __init__(self, r, config):
        # Set missing fields to default values
        self.d = {
            "type": r.get("type", None),
            "id": r.get("id", None),
            "attr": r.get("attr", None),
            "t1": r.get("t1", default_t1),
            "t2": r.get("t2", r.get("t1", default_t1)),
            "v": r.get("v", default_v),
            "c": r.get("c", default_c),
            "src": r.get("src", default_src)
        }

        # Validate the record
        validate_record(self, config)

    def __getitem__(self, item):
        return self.d[item]
