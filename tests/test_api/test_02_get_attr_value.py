from time import sleep

import common
from common import ACCEPTED_ERROR_CODES

from dp3.api.internal.entity_response_models import EntityEidAttrValueOrHistory

TESTED_PATH = "entity/{entity}/{eid}/get/{attr}"
TESTED_HISTORY_PATH = TESTED_PATH.format(
    entity="test_entity_type", eid="test_entity_id", attr="test_attr_history"
)
TESTED_INT_PATH = TESTED_PATH.format(
    entity="test_entity_type", eid="test_entity_id", attr="test_attr_int"
)


class GetEidAttrValue(common.APITest):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        sleep(8)
        # Data must pass to DB before this request is processed
        cls.get_request("control/make_snapshots")

    def test_unknown_entity_type(self):
        response = self.get_request(
            TESTED_PATH.format(entity="xyz", eid="test_entity_id", attr="test_attr_int")
        )
        self.assertIn(response.status_code, ACCEPTED_ERROR_CODES)

    def test_unknown_attr_name(self):
        response = self.get_request(
            TESTED_PATH.format(entity="test_entity_type", eid="test_entity_id", attr="xyz")
        )
        self.assertIn(response.status_code, ACCEPTED_ERROR_CODES)

    def test_unknown_entity_id(self):
        payload = self.get_entity_data(
            TESTED_PATH.format(entity="test_entity_type", eid="xyz", attr="test_attr_int"),
            EntityEidAttrValueOrHistory,
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
        self.query_expected_value(
            lambda: self.get_entity_data(TESTED_INT_PATH, EntityEidAttrValueOrHistory),
            lambda received: received == expected,
        )

    def test_invalid_timestamp(self):
        response = self.get_request(TESTED_HISTORY_PATH, date_from="xyz")
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
                response = self.get_request(TESTED_HISTORY_PATH, **intervals)
                self.assertEqual(200, response.status_code)

    def get_history_attr_value(self, path: str):
        result = self.get_entity_data(path, EntityEidAttrValueOrHistory)
        result.history = []  # Ignore history for sake of test reliability
        if isinstance(result.current_value, list):
            result.current_value = sorted(result.current_value, key=lambda x: str(x))
        return result

    def test_attr_serialization(self):
        # Test first with `query_expected_value` to await snapshot
        for data_type, valid in common.observation_values["valid"].items():
            with self.subTest(data_type=data_type, v=valid):
                path = TESTED_PATH.format(
                    entity="test_entity_type",
                    eid="test_entity_id",
                    attr=f"test_attr_{data_type}",
                )
                expected = EntityEidAttrValueOrHistory(
                    attr_type=2, current_value=sorted(valid, key=lambda x: str(x))
                )
                self.query_expected_value(
                    lambda: self.get_history_attr_value(path),  # noqa: B023
                    lambda received: received == expected,  # noqa: B023
                    attempts=50,
                    delay_s=0.2,
                )

        for data_type, valid in common.values["valid"].items():
            value = valid[-1]  # Plain attribute has the latest sent value
            with self.subTest(data_type=data_type, v=value):
                path = TESTED_PATH.format(
                    entity="test_entity_type",
                    eid="test_entity_id",
                    attr=f"test_attr_{data_type}",
                )
                expected = EntityEidAttrValueOrHistory(attr_type=1, current_value=value)
                result = self.get_entity_data(path, EntityEidAttrValueOrHistory)
                if expected != result:
                    print(f"Expected: {expected}")
                    print(f"Result:   {result}")
                self.assertEqual(expected, result)
