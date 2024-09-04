import common

from dp3.api.internal.entity_response_models import EntityEidAttrValue, EntityEidAttrValueOrHistory

TESTED_PATH = "entity/test_entity_type/eid01/{action}/test_attr_string"


class SetEidAttrValue(common.APITest):
    def test_invalid_payload(self):
        response = self.post_request(TESTED_PATH.format(action="set"), json={})
        self.assertEqual(response.status_code, 422, msg=response.json())

    def test_valid_payload(self):
        payload = EntityEidAttrValue(value="Test string val1")
        response = self.post_request(
            TESTED_PATH.format(action="set"), data=payload.model_dump_json()
        )
        self.assertEqual(response.status_code, 200, msg=response.json())

        expected = EntityEidAttrValueOrHistory(attr_type=1, current_value="Test string val1")
        self.query_expected_value(
            lambda: self.get_entity_data(
                TESTED_PATH.format(action="get"), EntityEidAttrValueOrHistory
            ),
            lambda received: received == expected,
        )
