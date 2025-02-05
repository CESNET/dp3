import sys
from time import sleep
from typing import Any

from common import ACCEPTED_ERROR_CODES, APITest
from pydantic import RootModel

TESTED_PATH = "entity/{entity}/_/distinct/{attr}"
TESTED_INT_PATH = TESTED_PATH.format(entity="test_entity_type", attr="test_attr_int")
TESTED_SET_PATH = TESTED_PATH.format(entity="test_entity_type", attr="test_attr_set")

# Pydantic-based type for dictionary
GenericDictModel = RootModel[dict[Any, int]]


class GetDistinctAttrValues(APITest):
    @classmethod
    def setUpClass(cls) -> None:
        dps_attr_value = {
            "test_attr_int": 123,
            "test_attr_set": [123, 456],
        }

        # Push datapoints
        for attr, attr_val in dps_attr_value.items():
            res = cls.push_datapoints(
                [
                    {
                        "type": "test_entity_type",
                        "id": "test_entity_id",
                        "attr": attr,
                        "v": attr_val,
                    }
                ]
            )
            print(res.content.decode("utf-8"), file=sys.stderr)
            if res.status_code != 200:
                raise Exception(f"Failed to push datapoints: {res.status_code}")
        sleep(9)

        # Make snapshots
        cls.get_request("control/make_snapshots")
        sleep(6)

    def test_unknown_entity_type(self):
        response = self.get_request(TESTED_PATH.format(entity="xyz", attr="test_attr_int"))
        self.assertIn(response.status_code, ACCEPTED_ERROR_CODES)

    def test_unknown_attr_name(self):
        response = self.get_request(TESTED_PATH.format(entity="test_entity_type", attr="xyz"))
        self.assertIn(response.status_code, ACCEPTED_ERROR_CODES)

    def test_valid_get_int(self):
        self.query_expected_value(
            lambda: self.get_entity_data(TESTED_INT_PATH, GenericDictModel),
            lambda received: "123" in received.root,
        )

    def test_valid_get_set(self):
        self.query_expected_value(
            lambda: self.get_entity_data(TESTED_SET_PATH, GenericDictModel),
            lambda received: "123" in received.root and "456" in received.root,
        )
