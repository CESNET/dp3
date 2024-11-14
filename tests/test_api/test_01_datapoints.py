import json
import sys
from datetime import datetime
from typing import Any

import common
from common import ACCEPTED_ERROR_CODES


class PushDatapoints(common.APITest):
    def test_invalid_payload(self):
        response = self.push_datapoints(
            {"type": "test_entity_type", "attr": "test_attr_int", "id": "test_entity_id", "v": 123}
        )
        self.assertIn(response.status_code, ACCEPTED_ERROR_CODES, "invalid payload (not a list)")

        response = self.push_datapoints(["xyz"])
        self.assertIn(
            response.status_code,
            ACCEPTED_ERROR_CODES,
            "invalid payload (list element is not a dictionary)",
        )

    def test_invalid_content_type(self):
        response = self.post_request("datapoints", data="bodyisstring")
        self.assertIn(response.status_code, ACCEPTED_ERROR_CODES, "invalid payload (not a list)")

    def test_unknown_entity_type(self):
        response = self.push_datapoints(
            [{"type": "xyz", "attr": "test_attr_int", "id": "test_entity_id", "v": 123}]
        )
        self.assertIn(response.status_code, ACCEPTED_ERROR_CODES)

    def test_unknown_attr_name(self):
        response = self.push_datapoints(
            [{"type": "test_entity_type", "id": "test_entity_id", "attr": "xyz", "v": 123}]
        )
        self.assertIn(response.status_code, ACCEPTED_ERROR_CODES)

    def test_invalid_timestamp(self):
        response = self.push_datapoints(
            [
                {
                    "type": "test_entity_type",
                    "id": "test_entity_id",
                    "attr": "test_attr_history",
                    "v": 123,
                    "t1": "xyz",
                }
            ]
        )
        self.assertIn(response.status_code, ACCEPTED_ERROR_CODES)

    def test_missing_value(self):
        response = self.push_datapoints(
            [{"type": "test_entity_type", "id": "test_entity_id", "attr": "test_attr_int"}]
        )
        self.assertIn(response.status_code, ACCEPTED_ERROR_CODES)

    @staticmethod
    def make_datapoint(data_type: str, value: Any) -> dict[str, Any]:
        return {
            "type": "test_entity_type",
            "id": "test_entity_id",
            "attr": f"test_attr_{data_type}",
            "v": value,
        }

    def make_observation_datapoint(self, data_type: str, value: Any) -> dict[str, Any]:
        dp = self.make_datapoint(data_type, value)
        dp["t1"] = datetime.utcnow().isoformat()
        return dp

    def helper_test_datatype_value(self, datapoint: dict, expected_codes: set[int]):
        response = self.push_datapoints([datapoint])

        if response.status_code not in expected_codes:
            print(json.dumps(response.json(), indent=2), file=sys.stderr)
        self.assertIn(response.status_code, expected_codes)

    def test_data_type_values_valid(self):
        for data_type, valid in common.values["valid"].items():
            for value in valid:
                with self.subTest(data_type=data_type, v=value):
                    dp = self.make_datapoint(data_type, value)
                    self.helper_test_datatype_value(dp, expected_codes={200})

        for data_type, valid in common.observation_values["valid"].items():
            for value in valid:
                with self.subTest(data_type=data_type, v=value):
                    dp = self.make_observation_datapoint(data_type, value)
                    self.helper_test_datatype_value(dp, expected_codes={200})

    def test_data_type_values_invalid(self):
        for data_type, valid in common.values["invalid"].items():
            for value in valid:
                with self.subTest(data_type=data_type, v=value):
                    dp = self.make_datapoint(data_type, value)
                    self.helper_test_datatype_value(dp, expected_codes=ACCEPTED_ERROR_CODES)

        for data_type, valid in common.observation_values["invalid"].items():
            for value in valid:
                with self.subTest(data_type=data_type, v=value):
                    dp = self.make_observation_datapoint(data_type, value)
                    self.helper_test_datatype_value(dp, expected_codes=ACCEPTED_ERROR_CODES)
