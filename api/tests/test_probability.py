import unittest
import requests
import datetime
import json

base_url = 'http://127.0.0.1:5000/'

invalid_values = [
    {"A": "A", "B": "B", "C": "C", "D": "D"},
    "'xs'",
    {1: 0.6, 2: 0.3},
]


class ProbabilityAttrSingle(unittest.TestCase):
    @staticmethod
    def helper_send_to_single(v):
        def request(path, *args):
            try:
                args_str = '&'.join(args)
                if args_str != "":
                    args_str = f"?{args_str}"
                return requests.post(f"{base_url}/{path}{args_str}")
            except Exception as e:
                return f"error: {e}"

        print(f"Testing v={v}")
        response = request(f"test_entity_type/test_entity_id/test_attr_probability", f"v={json.dumps(v)}")
        print(f"Response: {response.content}")
        return response

    def test_valid_format(self):
        v = {"A": 0.6, "B": 0.3, "C": 0.05, "D": 0.05}
        response = self.helper_send_to_single(v)
        self.assertEqual(200, response.status_code)

    def test_invalid_format(self):
        for v in invalid_values:
            response = self.helper_send_to_single(v)
            self.assertEqual(400, response.status_code)


class ProbabilityAttrMultiple(unittest.TestCase):
    @staticmethod
    def helper_send_to_multiple(v):
        print(f"Testing v={v}")
        response = requests.post(f"{base_url}/datapoints", json=[{
            "type": "test_entity_type", "id": "test_entity_id", "attr": "test_attr_probability",
            "t1": datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S"), "v": json.dumps(v)
        }])
        print(f"Response: {response.content}")
        return response

    def test_valid_format_multiple(self):
        v = {"A": 0.6, "B": 0.3, "C": 0.05, "D": 0.05}
        response = self.helper_send_to_multiple(v)
        self.assertEqual(200, response.status_code)

    def test_invalid_format_multiple(self):
        for v in invalid_values:
            response = self.helper_send_to_multiple(v)
            self.assertEqual(400, response.status_code)
