import sys
import traceback
import requests

from common import retry_request_on_error
from test_get_history import test_get_history

base_url = None
verbose = None

test_suite = [
    test_get_history
]


def log(msg, verbose_lvl):
    global verbose
    if verbose >= verbose_lvl:
        print(msg)


if __name__ == "__main__":
    try:
        base_url = sys.argv[1]
        verbose = int(sys.argv[2])
    except Exception as e:
        print(f"usage: {sys.argv[0]} <base_url> <enable_verbose>")
        exit()

    # Test the base endpoint is live before running tests.
    retry_request_on_error(lambda : requests.get(base_url))

    for test in test_suite:
        log(test.__name__, 1)
        try:
            test(base_url, verbose)
            log("PASS", 1)
        except Exception as e:
            traceback.print_exc()
            log("FAIL", 1)
