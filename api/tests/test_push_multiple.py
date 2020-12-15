import requests
import common

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


def test_push_multiple(url, v):
    global verbose
    global base_url
    verbose = v
    base_url = url

    expected = "<Response [400]>"

    log("   invalid payload (not a list)", 2)
    response = request("datapoints", {"type": "test_entity_type", "attr": "test_attr_int", "id": "test_entity_id", "v": 123})
    check_response(expected, response)

    log("   invalid payload (list element is not a dictionary)", 2)
    response = request("datapoints", ["xyz"])
    check_response(expected, response)

    log("   unknown entity type", 2)
    response = request("datapoints", [{"type": "xyz", "attr": "test_attr_int", "id": "test_entity_id", "v": 123}])
    check_response(expected, response)

    # log("   unknown entity id")
    # response = request("datapoints", [{"id": "xyz", "type": "test_entity_type", "attr": "test_attr_int", "v": 123}])  # TODO int id
    # check_response(expected, response)

    log("   unknown attr name", 2)
    response = request("datapoints", [{"type": "test_entity_type", "id": "test_entity_id", "attr": "xyz", "v": 123}])
    check_response(expected, response)

    log("   invalid timestamp", 2)
    response = request("datapoints", [{"type": "test_entity_type", "id": "test_entity_id", "attr": "test_attr_history", "v": 123, "t1": "xyz"}])
    check_response(expected, response)

    log("   missing value", 2)
    response = request("datapoints", [{"type": "test_entity_type", "id": "test_entity_id", "attr": "test_attr_int"}])
    check_response(expected, response)

    expected = "<Response [200]>"

    log("   missing value (tag)", 2)
    response = request("datapoints", [{"type": "test_entity_type", "id": "test_entity_id", "attr": "test_attr_tag"}])
    check_response(expected, response)

    for data_type in common.data_types:
        valid = common.values["valid"][data_type]
        invalid = common.values["invalid"][data_type]

        for v in valid:
            log(f"   {data_type} | v={v}", 2)
            expected = "<Response [200]>"
            response = request("datapoints", [{"type": "test_entity_type", "id": "test_entity_id", "attr": f"test_attr_{data_type}", "v": v}])
            check_response(expected, response)

        for v in invalid:
            log(f"   {data_type} | v={v}", 2)
            expected = "<Response [400]>"
            response = request("datapoints", [{"type": "test_entity_type", "id": "test_entity_id", "attr": f"test_attr_{data_type}", "v": v}])
            check_response(expected, response)
