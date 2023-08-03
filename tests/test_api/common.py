import logging
import os
import sys
import time
import unittest
from typing import Callable, TypeVar

import requests
from pydantic import BaseModel

from dp3.common.config import ModelSpec, read_config_dir

Model = TypeVar("Model", bound=BaseModel)  # Can be any subtype of BaseModel
A = TypeVar("A")

base_url = os.getenv("BASE_URL", default="http://127.0.0.1:5000")
RECONNECT_DELAYS = [1, 2, 5, 10, 30]

ACCEPTED_ERROR_CODES = {400, 422}


class ConfigEnv(BaseModel):
    """Configuration environment variables container"""

    CONF_DIR: str


conf_env = ConfigEnv.parse_obj(os.environ)
CONFIG = read_config_dir(conf_env.CONF_DIR, recursive=True)
MODEL_SPEC = ModelSpec(CONFIG.get("db_entities"))

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
        "link": [{"eid": "test_entity_2"}],
        "data_link": [{"eid": "test_entity_3", "data": 42}],
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
        "category": ["xyz"],
        "array": ["xyz", '["xyz"]'],
        "set": ["xyz", '["xyz"]'],
        "dict": ["xyz", '{"xyz":"xyz"}', '{"key1":"xyz","key2":"xyz"}'],
        "link": ["test_entity_2", {"id": "test_entity_2"}],
        "data_link": [
            "test_entity_2",
            {"id": "test_entity_2"},
            {"eid": "test_entity_3"},
            {"eid": "test_entity_3", "data": [42]},
        ],
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
    @classmethod
    def setUpClass(cls) -> None:
        # Test the base endpoint is live before running tests.
        try:
            retry_request_on_error(lambda: requests.get(base_url))
            api_up = True
        except requests.exceptions.ConnectionError:
            api_up = False
        assert api_up, "API is down."

    @staticmethod
    def post_request(path, **kwargs) -> requests.Response:
        return retry_request_on_error(
            lambda: requests.post(f"{base_url}/{path}", **kwargs, timeout=5)
        )

    def query_expected_value(
        self, query: Callable[[], A], assertion: Callable[[A], bool], attempts=5, delay_s=0.5
    ):
        payload: A = None
        for _ in range(attempts):
            time.sleep(delay_s)
            payload = query()
            print(payload, file=sys.stderr)
            if assertion(payload):
                break
        self.assertTrue(assertion(payload))
        return payload

    @classmethod
    def push_datapoints(cls, json_data) -> requests.Response:
        return cls.post_request("datapoints", json=json_data)

    def get_entity_data(self, path: str, model: type[Model], **kwargs) -> Model:
        response = self.get_request(path, **kwargs)
        self.assertEqual(response.status_code, 200)
        return model.parse_raw(response.content)

    @staticmethod
    def get_request(path, **kwargs) -> requests.Response:
        args_str = "&".join([f"{key}={value}" for key, value in kwargs.items()])
        if args_str:
            args_str = f"?{args_str}"
        return retry_request_on_error(
            lambda: requests.get(f"{base_url}/{path}{args_str}", timeout=5)
        )
