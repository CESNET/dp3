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
