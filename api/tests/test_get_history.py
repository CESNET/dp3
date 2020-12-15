import requests

base_url = None
verbose = None


def log(msg, verbose_lvl):
    global verbose
    if verbose >= verbose_lvl:
        print(msg)


def request(path, *args):
    try:
        args_str = '&'.join(args)
        if args_str != "":
            args_str = f"?{args_str}"
        return requests.get(f"{base_url}/{path}{args_str}")
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


def test_get_history(url, v):
    global verbose
    global base_url
    verbose = v
    base_url = url

    expected = "<Response [400]>"

    log("   unknown entity type", 2)
    response = request(f"xyz/test_entity_id/test_attr_history/history")
    check_response(expected, response)

    log("   unknown attr name", 2)
    response = request(f"test_entity_type/test_entity_id/xyz/history")
    check_response(expected, response)

    log("   invalid timestamp", 2)
    response = request(f"test_entity_type/test_entity_id/test_attr_history/history", "t1=xyz")
    check_response(expected, response)

    expected = "<Response [200]>"

    log("   unknown entity id (no records)", 2)
    response = request(f"test_entity_type/xyz/test_attr_history/history")  # TODO int id
    check_response(expected, response)

    log("   valid time interval <T1, T2>", 2)
    response = request(f"test_entity_type/test_entity_id/test_attr_history/history", "t1=2020-01-01T00:00:00", "t2=2020-01-01T00:00:00")
    check_response(expected, response)

    log("   valid time interval <T1, _>", 2)
    response = request(f"test_entity_type/test_entity_id/test_attr_history/history", "t1=2020-01-01T00:00:00")
    check_response(expected, response)

    log("   valid time interval <_, T2>", 2)
    response = request(f"test_entity_type/test_entity_id/test_attr_history/history", "t2=2020-01-01T00:00:00")
    check_response(expected, response)

    log("   valid time interval <_, _>", 2)
    response = request(f"test_entity_type/test_entity_id/test_attr_history/history")
    check_response(expected, response)

    expected = "<Response [400]>"

    log("   invalid time interval (T1 > T2)", 2)
    response = request(f"test_entity_type/test_entity_id/test_attr_history/history", "t1=2020-01-01T00:00:01", "t2=2020-01-01T00:00:00")
    check_response(expected, response)
