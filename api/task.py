import sys
import os
sys.path.insert(1, os.path.join(os.path.dirname(__file__), '..'))

from dp3.common.utils import parse_rfc_time

# TODO: why is this not part of dp3.common? Task abstraction would surely be useful elsewhere

# Error message templates
err_msg_type = "type of '{}' is invalid (must be '{}')"
err_msg_value = "value of '{}' is invalid"
err_msg_missing_field = "mandatory field '{}' is missing"

# Default task fields
default_attr_updates = []
default_events = []
default_data_points = []
default_create = True
default_delete = False
default_src = ""
default_tags = []

valid_operations = [
    "set",
    "unset",
    "add",
    "sub",
    "setmax",
    "setmin",
    "next_step",
    "array_append",
    "array_insert",
    "array_remove",
    "set_add",
    "rem_from_set"
]


# Validate record fields according to entity/attribute specification
def validate_task(task, config):
    # Check mandatory fields and their data types
    assert task.get("etype") is not None, err_msg_missing_field.format("etype")
    assert task.get("ekey") is not None, err_msg_missing_field.format("ekey")

    # Check data type of task fields
    assert type(task.get("etype")) is str, err_msg_type.format("type", "str")
    assert type(task.get("attr_updates")) is list, err_msg_type.format("attr_updates", "list")
    assert type(task.get("events")) is list, err_msg_type.format("events", "list")
    assert type(task.get("data_points")) is list, err_msg_type.format("data_points", "list")
    assert type(task.get("create")) is bool, err_msg_type.format("create", "bool")
    assert type(task.get("delete")) is bool, err_msg_type.format("delete", "bool")
    assert type(task.get("src")) is str, err_msg_type.format("src", "str")
    assert type(task.get("tags")) is list, err_msg_type.format("tags", "list")

    # Check specification of given entity type
    assert task["etype"] in config, f"no specification found for entity type '{task['etype']}'"

    entity_spec = config[task["etype"]]["entity"]
    attr_spec = config[task["etype"]]["attribs"]

    # Check data type of entity key - validator functions are specified in entityspec module
    assert entity_spec.key_validator(task["ekey"]), err_msg_type.format("ekey", entity_spec.key_data_type)

    # Validate attribute updates
    assert type(task["attr_updates"]) is list, err_msg_type.format("attr_updates", "list")
    for item in task["attr_updates"]:
        # Check data type of attr update structure (dict)
        assert type(item) is dict, err_msg_type.format("attr update", "dict")

        # Check specification for given attribute
        assert item["attr"] in attr_spec, f"no specification found for attribute '{item['attr']}'"

        # TODO Platform uses bool data type for tag attributes -> either enforce bool data type in api, or set value to True regardless of the actual value
        if attr_spec[item["attr"]].data_type == "tag":
            item["val"] = True

        # Check mandatory fields
        assert "attr" in item, err_msg_missing_field.format("attr")
        assert "op" in item, err_msg_missing_field.format("op")
        assert "val" in item, err_msg_missing_field.format("val")

        # Check data type of attr update fields
        assert type(item["attr"]) is str, err_msg_type.format("attr", "str")
        assert type(item["op"]) is str, err_msg_type.format("op", "str")
        assert attr_spec[item["attr"]].value_validator(item["val"]), err_msg_type.format("val", attr_spec[item["attr"]].data_type)

        # Check valid operation
        assert item["op"] in valid_operations, err_msg_value.format("op")

    # Validate events
    assert type(task["attr_updates"]) is list, err_msg_type.format("events", "list")
    for item in task["events"]:
        assert type(item) is str, err_msg_type.format("event", "string")
        # TODO should receiver validate events?

    # Validate data points
    assert type(task["data_points"]) is list, err_msg_type.format("data_points", "list")
    for item in task["data_points"]:
        # Check data type of data point structure (dict)
        assert type(item) is dict, err_msg_type.format("datapoint", "dict")

        # Check specification for given attribute
        assert item["attr"] in attr_spec, f"no specification found for attribute '{item['attr']}'"

        # TODO Platform uses bool data type for tag attributes -> either enforce bool data type in api, or set value to True regardless of the actual value
        if attr_spec[item["attr"]].data_type == "tag":
            item["v"] = True

        # Check mandatory fields
        assert "attr" in item, err_msg_missing_field.format("attr")
        assert "t1" in item, err_msg_missing_field.format("t1")
        assert "v" in item, err_msg_missing_field.format("v")

        # Set optional fields to default values if needed
        if "t2" not in item:
            item["t2"] = item["t1"]
        if "c" not in item:
            item["c"] = 1.0

        # Check data type of data-point fields
        assert type(item["attr"]) is str, err_msg_type.format("attr", "str")
        assert type(item["t1"]) is str, err_msg_type.format("t1", "str")
        assert type(item["t2"]) is str, err_msg_type.format("t2", "str")
        assert type(item["c"]) is float, err_msg_type.format("c", "float")
        assert attr_spec[item["attr"]].value_validator(item["v"]), err_msg_type.format("v", attr_spec[item["attr"]].data_type)

        # Check timestamp format
        try:
            t1 = parse_rfc_time(item["t1"])
            t2 = parse_rfc_time(item["t2"])
        except ValueError:
            raise TypeError("invalid timestamp format")

        # Check valid interval (T2 must be greater than or equal to T1)
        assert t1 <= t2, "t2 is less than t1"


class Task:
    # Create a task (from JSON/dict) according to entity/attribute specification
    # Raises an error if the task is invalid
    def __init__(self, t, config):
        # Set missing fields to default values
        self.d = {
            "etype": t.get("etype", None),
            "ekey": t.get("ekey", None),
            "attr_updates": t.get("attr_updates", default_attr_updates),
            "events": t.get("events", default_events),
            "data_points": t.get("data_points", default_data_points),
            "create": t.get("create", default_create),
            "delete": t.get("delete", default_delete),
            "src": t.get("src", default_src),
            "tags": t.get("tags", default_tags)
        }

        # Validate the task
        validate_task(self, config)

    def __getitem__(self, item):
        return self.d[item]

    def get(self, item):
        return self.d.get(item)

