
data_types = [
    "binary",
    "int",
    "int64",
    "string",
    "float",
    "ipv4",
    "ipv6",
    "mac",
    "time",
    "json",
    "category",
    "array",
    "set",
    "dict"
]

values = {
    "valid": {
        "binary": ["true"],
        "int": ["123"],
        "int64": ["123"],
        "string": ["\"xyz\""],
        "float": ["1.0", "1"],
        "ipv4": ["\"127.0.0.1\""],
        "ipv6": ["\"2001:0db8:85a3:0000:0000:8a2e:0370:7334\"", "\"::1\""],
        "mac": ["\"de:ad:be:ef:ba:be\"", "\"11:22:33:44:55:66\""],
        "time": ["\"2020-01-01T00:00:00\""],
        "json": ["{\"test\": \"test\"}"],
        "category": ["\"cat1\""],
        "array": ["[1,2,3]"],
        "set": ["[1,2,3]"],
        "dict": ["{\"key1\":1,\"key2\":\"xyz\"}"]
    },
    "invalid": {
        "binary": ["xyz"],
        "int": ["xyz"],
        "int64": ["xyz"],
        "string": ["xyz"],  # missing double quotes
        "float": ["xyz"],
        "ipv4": ["\"xyz\""],
        "ipv6": ["\"xyz\""],
        "mac": ["\"xyz\""],
        "time": ["\"xyz\""],
        "json": ["xyz"],
        "category": ["\"xyz\""],
        "array": ["xyz", "[\"xyz\"]"],
        "set": ["xyz", "[\"xyz\"]"],
        "dict": ["xyz", "{\"xyz\":\"xyz\"}", "{\"key1\":\"xyz\",\"key2\":\"xyz\"}"]
    }
}
