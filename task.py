import sys
import os
import time
sys.path.insert(1, os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../processing_platform')))

from src.common.utils import parse_rfc_time

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

# Validate record fields according to entity/attribute specification
def validate_task(task, config):
    # Check mandatory fields
    assert task["etype"] is not None, err_msg_missing_field.format("etype")
    assert task["ekey"] is not None, err_msg_missing_field.format("ekey")

    # Check data type of task fields
    assert type(task["etype"]) is str, err_msg_type.format("type", "str")
    assert type(task["attr_updates"]) is list, err_msg_type.format("attr_updates", "list")
    assert type(task["events"]) is list, err_msg_type.format("events", "list")
    assert type(task["data_points"]) is list, err_msg_type.format("data_points", "list")
    assert type(task["create"]) is bool, err_msg_type.format("create", "bool")
    assert type(task["delete"]) is bool, err_msg_type.format("delete", "bool")
    assert type(task["src"]) is str, err_msg_type.format("src", "str")
    assert type(task["tags"]) is list, err_msg_type.format("tags", "list")

    # Check specification of given entity type
    assert task["etype"] in config, f"no specification found for entity type '{task['etype']}'"

    entity_spec = config[task["etype"]]["entity"]
    attr_spec = config[task["etype"]]["attribs"]

    # Validate attribute updates
    assert type(task["attr_updates"]) is list, err_msg_type.format("attr_updates", "list")
    for item in task["attr_updates"]:
        assert type(item) is dict, err_msg_type.format("attr update", "dict")

        # Check specification for given attribute
        assert item["attr"] in attr_spec, f"no specification found for attribute '{item['attr']}'"

        # Check mandatory fields
        assert "attr" in item, err_msg_missing_field.format("attr")
        assert "op" in item, err_msg_missing_field.format("op")
        assert "val" in item, err_msg_missing_field.format("val")

        # Check data type of update fields
        assert type(item["attr"]) is str, err_msg_type.format("attr", "str")
        assert type(item["op"]) is str, err_msg_type.format("op", "str")
        assert attr_spec[item["attr"]].value_validator(item["val"]), err_msg_type.format("val", attr_spec[item["attr"]].data_type)

    # Validate events
    assert type(task["attr_updates"]) is list, err_msg_type.format("events", "list")
    for item in task["events"]:
        assert type(item) is str, err_msg_type.format("event", "string")
        # TODO should receiver validate events?

    # Validate data points
    assert type(task["data_points"]) is list, err_msg_type.format("data_points", "list")
    for item in task["data_points"]:
        assert type(item) is dict, err_msg_type.format("datapoint", "dict")

        # Check mandatory fields
        assert item["attr"] is not None, err_msg_missing_field.format("attr")
        assert item["t1"] is not None, err_msg_missing_field.format("t1")

        # Set missing optional fields to default values
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
        except:
            raise TypeError("invalid timestamp")

        # Check valid interval (T2 must be greater than or equal to T1)
        assert t1 <= t2, err_msg_value.format("t2")



    # Check data type of entity key - validator functions are specified in entityspec module
    assert entity_spec.key_validator(task["ekey"]), err_msg_type.format("ekey", entity_spec.key_data_type)


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
