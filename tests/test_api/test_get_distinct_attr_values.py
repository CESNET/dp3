from typing import Any

from common import ACCEPTED_ERROR_CODES, APITest
from pydantic import RootModel

TESTED_PATH = "entity/{entity}/_/distinct/{attr}"
TESTED_INT_PATH = TESTED_PATH.format(entity="test_entity_type", attr="test_attr_int")
TESTED_SET_PATH = TESTED_PATH.format(entity="test_entity_type", attr="test_attr_set")

# Pydantic-based type for dictionary
GenericDictModel = RootModel[dict[Any, int]]


class GetDistinctAttrValues(APITest):
    def test_unknown_entity_type(self):
        response = self.get_request(TESTED_PATH.format(entity="xyz", attr="test_attr_int"))
        self.assertIn(response.status_code, ACCEPTED_ERROR_CODES)

    def test_unknown_attr_name(self):
        response = self.get_request(TESTED_PATH.format(entity="test_entity_type", attr="xyz"))
        self.assertIn(response.status_code, ACCEPTED_ERROR_CODES)

    def test_valid_get_int(self):
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
        self.query_expected_value(
            lambda: self.get_entity_data(TESTED_INT_PATH, GenericDictModel),
            lambda received: "123" in received.root,
        )

    def test_valid_get_set(self):
        response = self.push_datapoints(
            [
                {
                    "type": "test_entity_type",
                    "id": "test_entity_id",
                    "attr": "test_attr_set",
                    "v": [123, 456],
                }
            ]
        )

        self.assertEqual(200, response.status_code, "setup push failed")
        self.query_expected_value(
            lambda: self.get_entity_data(TESTED_SET_PATH, GenericDictModel),
            lambda received: "123" in received.root and "456" in received.root,
        )
