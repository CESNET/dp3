import datetime
import json
import logging
import os
import time
import unittest

import requests

base_url = os.getenv("BASE_URL", default='http://127.0.0.1:5000/')
api_up = None
MAX_RETRY_ATTEMPTS = 5

invalid_values = [
    {"A": "A", "B": "B", "C": "C", "D": "D"},
    "'xs'",
]

non_probability_datapoint = {
    "type": "test_entity_type", "id": "test_entity_id", "attr": "test_attr_history",
    "t1": datetime.datetime.now().isoformat(),
    "v": 42
}


def retry_request_on_error(request):
    for attempt in range(MAX_RETRY_ATTEMPTS):
        try:
            return request()
        except requests.exceptions.ConnectionError as err:
            logging.warning("Connection failed, retrying (attempt %d)", attempt + 1)
            time.sleep(5)
            if attempt + 1 == MAX_RETRY_ATTEMPTS:
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


class ProbabilityAttrSingle(APITest):
    @staticmethod
    def helper_send_to_single(v):
        def request(path, *args):
            args_str = '&'.join(args)
            if args_str != "":
                args_str = f"?{args_str}"
            return requests.post(f"{base_url}/{path}{args_str}", timeout=5)

        response = request(f"test_entity_type/test_entity_id/test_attr_probability", f"v={v}")
        return response

    def test_valid_format(self):
        v = {"A": 0.6, "B": 0.3, "C": 0.05, "D": 0.05}
        response = self.helper_send_to_single(json.dumps(v))
        self.assertEqual(200, response.status_code)

    def test_invalid_format(self):
        for v in invalid_values:
            with self.subTest(v=v):
                response = self.helper_send_to_single(json.dumps(v))
                self.assertEqual(400, response.status_code)

    def test_invalid_json(self):
        v = "{'A':1.0}"
        response = self.helper_send_to_single(v)
        self.assertEqual(400, response.status_code)


class ProbabilityAttrMultiple(APITest):
    @staticmethod
    def helper_send_to_multiple(v):
        response = requests.post(f"{base_url}/datapoints", json=[{
            "type": "test_entity_type", "id": "test_entity_id", "attr": "test_attr_probability",
            "t1": datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S"), "v": v
        }, non_probability_datapoint], timeout=5)
        return response

    def test_valid_format_multiple(self):
        v = {"A": 0.6, "B": 0.3, "C": 0.05, "D": 0.05}
        response = self.helper_send_to_multiple(json.dumps(v))
        self.assertEqual(200, response.status_code)

    def test_invalid_format_multiple(self):
        for v in invalid_values:
            with self.subTest(v=v):
                response = self.helper_send_to_multiple(json.dumps(v))
                self.assertEqual(400, response.status_code)

    def test_invalid_json(self):
        v = "{'A':1.0}"
        response = self.helper_send_to_multiple(v)
        self.assertEqual(400, response.status_code)
