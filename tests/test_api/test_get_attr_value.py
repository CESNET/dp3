import sys
import time

import common
from common import ACCEPTED_ERROR_CODES

from api.internal.entity_response_models import EntityEidAttrValueOrHistory


class GetEidAttrValue(common.APITest):
    def test_unknown_entity_type(self):
        response = self.get_entity("xyz/test_entity_id/get/test_attr_int")
        self.assertIn(response.status_code, ACCEPTED_ERROR_CODES)

    def test_unknown_attr_name(self):
        response = self.get_entity("test_entity_type/test_entity_id/get/xyz")
        self.assertIn(response.status_code, ACCEPTED_ERROR_CODES)

    def test_unknown_entity_id(self):
        payload = self.get_entity_data(
            "test_entity_type/xyz/get/test_attr_int", EntityEidAttrValueOrHistory
        )
        expected = EntityEidAttrValueOrHistory(attr_type=1)
        self.assertEqual(payload, expected, "The returned object is not empty")

    def test_valid_get(self):
        response = self.push_datapoints(
            [
                {
                    "type": "test_entity_type",
                    "id": "test_entity_id",
                    "attr": "test_attr_int",
                    "v": 123,
                }
            ]
        )
        self.assertEqual(200, response.status_code, "setup push failed")
        expected = EntityEidAttrValueOrHistory(attr_type=1, current_value=123)
        payload = None
        for _ in range(5):
            time.sleep(0.5)
            payload = self.get_entity_data(
                "test_entity_type/test_entity_id/get/test_attr_int", EntityEidAttrValueOrHistory
            )
            print(payload, file=sys.stderr)
            if payload == expected:
                break
        self.assertEqual(payload, expected)

    def test_invalid_timestamp(self):
        response = self.get_entity(
            "test_entity_type/test_entity_id/get/test_attr_history", date_from="xyz"
        )
        self.assertEqual(422, response.status_code)

    def test_valid_time_intervals(self):
        time_intervals = {
            "<T1, T2>": {"date_from": "2020-01-01T00:00:00", "date_to": "2020-01-01T00:00:00"},
            "<T1, _>": {"date_from": "2020-01-01T00:00:00"},
            "<_, T2>": {"date_to": "2020-01-01T00:00:00"},
            "<_, _>": {},
        }
        for msg, intervals in time_intervals.items():
            with self.subTest(msg=msg):
                response = self.get_entity(
                    "test_entity_type/test_entity_id/get/test_attr_history", **intervals
                )
                self.assertEqual(200, response.status_code)
