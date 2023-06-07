import sys
import time

import common
from pydantic import BaseModel

from api.internal.entity_response_models import EntityEidAttrValueOrHistory

ACCEPTED_UNKNOWN_CODES = {400, 404, 422}


class GetValue(common.APITest):
    def test_unknown_entity_type(self):
        response = self.get_entity("xyz/test_entity_id/get/test_attr_int")
        self.assertIn(response.status_code, ACCEPTED_UNKNOWN_CODES)

    def test_unknown_attr_name(self):
        response = self.get_entity("test_entity_type/test_entity_id/get/xyz")
        self.assertIn(response.status_code, ACCEPTED_UNKNOWN_CODES)

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

    def get_entity_data(self, path: str, model: BaseModel) -> BaseModel:
        response = self.get_entity(path)
        self.assertEqual(response.status_code, 200)
        return model.parse_raw(response.content)
