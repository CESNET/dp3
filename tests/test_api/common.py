import json
import logging
import os
import time
import unittest

import requests

base_url = os.getenv("BASE_URL", default="http://127.0.0.1:5000")
api_up = None
RECONNECT_DELAYS = [1, 2, 5, 10, 30]

values = {
    "valid": {
        "binary": ["true"],
        "int": ["123"],
        "int64": ["123"],
        "string": ['"xyz"'],
        "float": ["1.0", "1"],
        "ipv4": ["127.0.0.1"],
        "ipv6": ["2001:0db8:85a3:0000:0000:8a2e:0370:7334", "::1"],
        "mac": ["de:ad:be:ef:ba:be", "11:22:33:44:55:66"],
        "time": ["2020-01-01T00:00:00"],
        "json": ['{"test": "test"}'],
        "category": ["cat1"],
        "array": [[1, 2, 3]],
        "set": [[1, 2, 3]],
        "dict": [{"key1": 1, "key2": "xyz"}],
        # TODO property datatype not yet implemented
        "probability": [json.dumps({"A": 0.6, "B": 0.3, "C": 0.05, "D": 0.05})],
    },
    "invalid": {
        "binary": ["xyz"],
        "int": ["xyz"],
        "int64": ["xyz"],
        "string": [],  # all JSON strings can be converted to strings
        "float": ["xyz"],
        "ipv4": ['"xyz"'],
        "ipv6": ['"xyz"'],
        "mac": ['"xyz"'],
        "time": ['"xyz"'],
        "json": ["xyz"],
        # TODO category validation is in progress
        # "category": ['"xyz"'],
        "array": ["xyz", '["xyz"]'],
        "set": ["xyz", '["xyz"]'],
        "dict": ["xyz", '{"xyz":"xyz"}', '{"key1":"xyz","key2":"xyz"}'],
        # TODO property datatype not yet implemented
        # "probability": [
        #     json.dumps({"A": "A", "B": "B", "C": "C", "D": "D"}), # not a probability distribution
        #     '"xyz"',  # invalid format (not a dict)
        #     "{'A':1.0}",
        # ],  # invalid JSON
    },
}


def retry_request_on_error(request):
    for attempt, delay in enumerate(RECONNECT_DELAYS):
        try:
            return request()
        except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout) as err:
            logging.warning("Connection failed, retrying in %ds (attempt %d)", delay, attempt + 1)
            time.sleep(delay)
            if attempt + 1 == len(RECONNECT_DELAYS):
                raise err


class APITest(unittest.TestCase):
    def setUp(self) -> None:
        # Test the base endpoint is live before running tests.
        global api_up
        if api_up is None:
            try:
                retry_request_on_error(lambda: requests.get(base_url))
                api_up = True
            except requests.exceptions.ConnectionError:
                api_up = False
        return self.assertTrue(api_up, msg="API is down.")

    @staticmethod
    def push_single(endpoint_path: str, **datapoint_values) -> requests.Response:
        args_str = "&".join([f"{key}={value}" for key, value in datapoint_values.items()])
        if args_str != "":
            args_str = f"?{args_str}"
        return retry_request_on_error(
            lambda: requests.post(f"{base_url}/{endpoint_path}{args_str}", timeout=5)
        )

    @staticmethod
    def request(path, json_data) -> requests.Response:
        return retry_request_on_error(
            lambda: requests.post(f"{base_url}/{path}", json=json_data, timeout=5)
        )

    def push_datapoints(self, json_data) -> requests.Response:
        return self.request("datapoints", json_data=json_data)

    def push_task(self, json_data) -> requests.Response:
        return self.request("tasks", json_data=json_data)

    def get_entity(self, path: str, **kwargs):
        return self.get_request(f"entity/{path}", **kwargs)

    @staticmethod
    def get_request(path, **kwargs) -> requests.Response:
        args_str = "&".join([f"{key}={value}" for key, value in kwargs.items()])
        if args_str != "":
            args_str = f"?{args_str}"
        return retry_request_on_error(
            lambda: requests.get(f"{base_url}/{path}{args_str}", timeout=5)
        )
