# Error message templates
err_msg_type = "type of '{}' is invalid (must be '{}')"

# List of supported key data types
supported_data_types = [
    "string",
    "int"
]

# This class represents specification of an entity type (e.g. ip, asn, ...)
class EntitySpec:
    def __init__(self, spec):
        # Set default values for missing fields
        self.id = spec.get("id", None)
        self.name = spec.get("name", self.id)
        self.key_data_type = spec.get("key_data_type", None)
        self.auto_create_record = spec.get("auto_create_record", False)

        # Check data type of specification fields
        assert type(self.id) is str, err_msg_type.format("id", "str")
        assert type(self.name) is str, err_msg_type.format("name", "str")
        assert type(self.key_data_type) is str, err_msg_type.format("key_data_type", "str")
        assert type(self.auto_create_record) is bool, err_msg_type.format("auto_create_record", "bool")

        # Key data type must be supported
        assert self.key_data_type in supported_data_types, f"key data type '{self.key_data_type}' is not supported"
