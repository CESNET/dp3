import requests

base_url = None
verbose = None


def log(msg, verbose_lvl):
    global verbose
    if verbose >= verbose_lvl:
        print(msg)


def request(path, json_data):
    try:
        return requests.post(f"{base_url}/{path}", json=json_data)
    except Exception as e:
        return f"error: {e}"


def check_response(expected, response):
    log(f"      expected: {expected}", 3)
    log(f"      response: {response} ({response.content})", 3)
    try:
        assert str(response) == expected
        log("   PASS", 2)
    except AssertionError:
        log("   FAIL", 2)
        raise Exception


def test_push_task(url, v):
    global verbose
    global base_url
    verbose = v
    base_url = url

    expected = "<Response [400]>"

    log("   unknown entity type", 2)
    response = request("tasks", {
        "etype": "xyz",
        "ekey": "test_entity_id"
    })
    check_response(expected, response)

    log("   invalid attr_updates (not a list)", 2)
    response = request("tasks", {
        "etype": "test_entity_type",
        "ekey": "test_entity_id",
        "attr_updates": {"attr": "test_attr_int", "op": "set", "val": 1}
    })
    check_response(expected, response)

    log("   invalid attr_updates (list element is not a dictionary)", 2)
    response = request("tasks", {
        "etype": "test_entity_type",
        "ekey": "test_entity_id",
        "attr_updates": ["xyz"]
    })
    check_response(expected, response)

    log("   invalid attr_updates (missing 'op' field)", 2)
    response = request("tasks", {
        "etype": "test_entity_type",
        "ekey": "test_entity_id",
        "attr_updates": [{"attr": "test_attr_int", "val": 1}]
    })
    check_response(expected, response)

    log("   invalid attr_updates (unknown attr name)", 2)
    response = request("tasks", {
        "etype": "test_entity_type",
        "ekey": "test_entity_id",
        "attr_updates": [{"attr": "xyz", "op": "set", "val": 1}]
    })
    check_response(expected, response)

    log("   invalid attr_updates (unknown operation)", 2)
    response = request("tasks", {
        "etype": "test_entity_type",
        "ekey": "test_entity_id",
        "attr_updates": [{"attr": "test_attr_int", "op": "xyz", "val": 1}]
    })
    check_response(expected, response)

    log("   invalid attr_updates (invalid value)", 2)
    response = request("tasks", {
        "etype": "test_entity_type",
        "ekey": "test_entity_id",
        "attr_updates": [{"attr": "test_attr_int", "op": "set", "val": "xyz"}]
    })
    check_response(expected, response)

    log("   invalid data_points (not a list)", 2)
    response = request("tasks", {
        "etype": "test_entity_type",
        "ekey": "test_entity_id",
        "data_points": {"attr": "test_attr_int", "v": 1, "t1": "2020-01-01T00:00:00"}
    })
    check_response(expected, response)

    log("   invalid attr_updates (list element is not a dictionary)", 2)
    response = request("tasks", {
        "etype": "test_entity_type",
        "ekey": "test_entity_id",
        "data_points": ["xyz"]
    })
    check_response(expected, response)

    log("   invalid data_points (missing 'attr' field)", 2)
    response = request("tasks", {
        "etype": "test_entity_type",
        "ekey": "test_entity_id",
        "data_points": [{"v": 1, "t1": "2020-01-01T00:00:00"}]
    })
    check_response(expected, response)

    log("   invalid data_points (invalid timestamp)", 2)
    response = request("tasks", {
        "etype": "test_entity_type",
        "ekey": "test_entity_id",
        "data_points": [{"attr": "test_attr_int", "v": 1, "t1": "xyz"}]
    })
    check_response(expected, response)

    log("   invalid data_points (invalid value)", 2)
    response = request("tasks", {
        "etype": "test_entity_type",
        "ekey": "test_entity_id",
        "data_points": [{"attr": "test_attr_int", "v": "xyz", "t1": "2020-01-01T00:00:00"}]
    })
    check_response(expected, response)

    log("   invalid data_points (invalid confidence)", 2)
    response = request("tasks", {
        "etype": "test_entity_type",
        "ekey": "test_entity_id",
        "data_points": [{"attr": "test_attr_int", "v": "xyz", "t1": "2020-01-01T00:00:00", "c": "xyz"}]
    })
    check_response(expected, response)

    log("   invalid events (not a list)", 2)
    response = request("tasks", {
        "etype": "test_entity_type",
        "ekey": "test_entity_id",
        "events": "xyz"
    })
    check_response(expected, response)

    log("   invalid events (list element is not a string)", 2)
    response = request("tasks", {
        "etype": "test_entity_type",
        "ekey": "test_entity_id",
        "events": [123]
    })
    check_response(expected, response)

    # TODO - API should probably validate events aswell
    """
    log("   invalid events (invalid event)", 2)
    response = request("tasks", {
        "etype": "test_entity_type",
        "ekey": "test_entity_id",
        "events": ["xyz"]
    })
    check_response(expected, response)
    """

    log("   invalid create", 2)
    response = request("tasks", {
        "etype": "test_entity_type",
        "ekey": "test_entity_id",
        "create": "xyz"
    })
    check_response(expected, response)

    log("   invalid delete", 2)
    response = request("tasks", {
        "etype": "test_entity_type",
        "ekey": "test_entity_id",
        "delete": "xyz"
    })
    check_response(expected, response)

    log("   invalid src", 2)
    response = request("tasks", {
        "etype": "test_entity_type",
        "ekey": "test_entity_id",
        "src": 123
    })
    check_response(expected, response)

    log("   invalid tags", 2)
    response = request("tasks", {
        "etype": "test_entity_type",
        "ekey": "test_entity_id",
        "tags": "xyz"
    })
    check_response(expected, response)