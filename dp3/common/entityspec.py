# Error message templates
from typing import Union

err_msg_type = "type of '{}' is invalid (must be '{}')"
err_msg_missing_field = "mandatory field '{}' is missing"

# List of supported key data types
supported_data_types = [
    "string",
    "int"
]

# Dictionary containing validator functions for supported data types
validators = {
    "string": lambda v: type(v) is str,
    "int": lambda v: type(v) is int
}

# This class represents specification of an entity type (e.g. ip, asn, ...)
class EntitySpec:
    # Class constructor
    # Raises AssertionError if the specification is invalid
    def __init__(self, id: str, spec: dict[str, Union[str, bool]]) -> None:
        # Set default values for missing fields
        self.id = id
        self.name = spec.get("name", self.id)
        self.key_data_type = spec.get("key_data_type", None)
        self.auto_create_record = spec.get("auto_create_record", False)

        # Check mandatory specification fields
        assert self.id is not None, err_msg_missing_field.format("id")
        assert self.key_data_type is not None, err_msg_missing_field.format("key_data_type")

        # Check data type of specification fields
        assert type(self.id) is str, err_msg_type.format("id", "str")
        assert type(self.name) is str, err_msg_type.format("name", "str")
        assert type(self.key_data_type) is str, err_msg_type.format("key_data_type", "str")
        assert type(self.auto_create_record) is bool, err_msg_type.format("auto_create_record", "bool")

        # Key data type must be supported
        assert self.key_data_type in supported_data_types, f"key data type '{self.key_data_type}' is not supported"

        # Initialize attribute's key validator function according to its data type
        self.key_validator = validators[self.key_data_type]

    def __repr__(self):
        o = {k: getattr(self, k) for k in ('name','key_data_type','auto_create_record')}
        return f"EntitySpec({self.id!r}, {o!r})"

