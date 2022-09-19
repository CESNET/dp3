import datetime
import json
from common import APITest


invalid_values = [
    {"A": "A", "B": "B", "C": "C", "D": "D"},
    "'xs'",
]

non_probability_datapoint = {
    "type": "test_entity_type", "id": "test_entity_id", "attr": "test_attr_history",
    "t1": datetime.datetime.now().isoformat(),
    "v": 42
}


class ProbabilityAttrSingle(APITest):
    path = "test_entity_type/test_entity_id/test_attr_probability"

    def test_valid_format(self):
        v = {"A": 0.6, "B": 0.3, "C": 0.05, "D": 0.05}
        response = self.helper_send_to_single(self.path, v=json.dumps(v))
        self.assertEqual(200, response.status_code)

    def test_invalid_format(self):
        for v in invalid_values:
            with self.subTest(v=v):
                response = self.helper_send_to_single(self.path, v=json.dumps(v))
                self.assertEqual(400, response.status_code)

    def test_invalid_json(self):
        v = "{'A':1.0}"
        response = self.helper_send_to_single(self.path, v=v)
        self.assertEqual(400, response.status_code)


class ProbabilityAttrMultiple(APITest):
    @staticmethod
    def value_to_json_data(v):
        return [{
            "type": "test_entity_type", "id": "test_entity_id", "attr": "test_attr_probability",
            "t1": datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S"), "v": json.dumps(v)
        }, non_probability_datapoint]

    def test_valid_format_multiple(self):
        v = {"A": 0.6, "B": 0.3, "C": 0.05, "D": 0.05}
        response = self.helper_send_to_multiple(self.value_to_json_data(v))
        self.assertEqual(200, response.status_code)

    def test_invalid_format_multiple(self):
        for v in invalid_values:
            with self.subTest(v=v):
                response = self.helper_send_to_multiple(self.value_to_json_data(v))
                self.assertEqual(400, response.status_code)

    def test_invalid_json(self):
        v = "{'A':1.0}"
        response = self.helper_send_to_multiple(self.value_to_json_data(v))
        self.assertEqual(400, response.status_code)
