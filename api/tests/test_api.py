import sys
import traceback

from test_push_single import test_push_single
from test_push_multiple import test_push_multiple
from test_push_task import test_push_task
from test_get_value import test_get_value
from test_get_history import test_get_history


base_url = None
verbose = None

test_suite = [
    test_push_single,
    test_push_multiple,
    test_push_task,
    test_get_value,
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

    for test in test_suite:
        log(test.__name__, 1)
        try:
            test(base_url, verbose)
            log("PASS", 1)
        except Exception as e:
            traceback.print_exc()
            log("FAIL", 1)
