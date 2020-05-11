import time

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

# Required timestamp format
# TODO
timestamp_format = ""

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
    assert task["etype"] in config, f"No specification found for entity type '{task['etype']}'"

    entity_spec = config[task["etype"]]["entity"]
    attr_spec = config[task["etype"]]["attribs"]

    # Validate attribute updates
    for item in task["attr_updates"]:
        # TODO
        pass

    # Validate events
    for item in task["events"]:
        # TODO
        pass

    # Validate data points
    for item in task["data_points"]:
        # TODO
        pass

    # Check data type of entity key - validator functions are specified in entityspec module
    assert entity_spec.key_validator(task["ekey"]), err_msg_value.format("etype", entity_spec.key_data_type)


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
