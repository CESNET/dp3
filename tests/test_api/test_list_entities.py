import common

from dp3.api.internal.entity_response_models import EntityState


class ListEntities(common.APITest):
    def test_get_entities(self):
        response = self.get_request("entities")
        self.assertEqual(200, response.status_code)

        received = response.json()
        self.assertIsInstance(received, dict)
        for key, item in received.items():
            received[key] = EntityState.parse_obj(item)
        self.assertListEqual(list(received.keys()), list(common.MODEL_SPEC.entities.keys()))
