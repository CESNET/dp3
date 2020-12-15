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


def test_get_value(url, v):
    global verbose
    global base_url
    verbose = v
    base_url = url

    expected = "<Response [400]>"

    log("   unknown entity type", 2)
    response = request(f"xyz/test_entity_id/test_attr_int")
    check_response(expected, response)

    log("   unknown attr name", 2)
    response = request(f"test_entity_type/test_entity_id/xyz")
    check_response(expected, response)

    expected = "<Response [404]>"

    log("   unknown entity id", 2)
    response = request(f"test_entity_type/xyz/test_attr_int")  # TODO int id
    check_response(expected, response)

    log("   int | no records", 2)
    response = request(f"test_entity_type/xyz/test_attr_int")
    check_response(expected, response)

    expected = "<Response [200]>"

    log("   int | v='123'", 2)
    response = request(f"test_entity_type/test_entity_id/test_attr_int")
    check_response(expected, response)
