import common

from dp3.api.internal.entity_response_models import EntityState
from dp3.common.context import entity_context
from dp3.common.entityspec import EntitySpec


class ListEntities(common.APITest):
    def test_get_entities(self):
        response = self.get_request("entities")
        self.assertEqual(200, response.status_code)

        received = response.json()
        self.assertIsInstance(received, dict)

        entities = {}
        # Validate received entity data
        for key, item in received.items():
            self.assertIsInstance(key, str)
            self.assertIsInstance(item, dict)

            entity_keys = set(item.keys()) & {"id", "id_data_type", "name"}
            self.assertSetEqual(entity_keys, {"id", "id_data_type", "name"})
            entities[key] = EntitySpec.model_validate(
                {k: v for k, v in item.items() if k in entity_keys} | {"snapshot": True}
            )
            for k in entity_keys:
                self.assertEqual(
                    entities[key].__getattribute__(k),
                    common.MODEL_SPEC.entities[key].__getattribute__(k),
                    f"Mismatch in entity '{key}' data at key '{k}'",
                )

        for key, item in received.items():
            with entity_context(entities[key], entities):
                received[key] = EntityState.model_validate(item)
        self.assertListEqual(list(received.keys()), list(common.MODEL_SPEC.entities.keys()))
