import time
import unittest

import common


@unittest.skip("Getting single value not implemented.")
class GetValue(common.APITest):
    def test_unknown_entity_type(self):
        response = self.get_request("xyz/test_entity_id/test_attr_int")
        self.assertEqual(400, response.status_code)

    def test_unknown_attr_name(self):
        response = self.get_request("test_entity_type/test_entity_id/xyz")
        self.assertEqual(400, response.status_code)

    def test_unknown_entity_id(self):
        response = self.get_request("test_entity_type/xyz/test_attr_int")
        self.assertEqual(404, response.status_code)

    def test_valid_get(self):
        response = self.push_multiple(
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
        time.sleep(2)
        response = self.get_request("test_entity_type/test_entity_id/test_attr_int")
        self.assertEqual(200, response.status_code)
