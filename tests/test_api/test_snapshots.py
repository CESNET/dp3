import datetime
from time import sleep

import common

from dp3.api.internal.entity_response_models import EntityEidData


class SnapshotIntegration(common.APITest):
    @classmethod
    def setUpClass(cls) -> None:
        now = datetime.datetime.now()
        t1 = (now - datetime.timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4]
        t2 = (now + datetime.timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4]

        entity_datapoints = [
            # For test_linked_entities_loaded
            {"type": "B", "id": "b1", "attr": "as", "v": "a1", "t1": t1, "t2": t2, "c": 1.0},
            {"type": "B", "id": "b1", "attr": "data1", "v": "initb"},
            {"type": "B", "id": "b1", "attr": "data2", "v": "initb"},
            {"type": "A", "id": "a1", "attr": "bs", "v": "b1", "t1": t1, "t2": t2, "c": 1.0},
            {"type": "A", "id": "a1", "attr": "data1", "v": "inita"},
            {"type": "A", "id": "a1", "attr": "data2", "v": "inita"},
            # For test_weak_component_detection
            {"type": "B", "id": "b2", "attr": "as", "v": "a1", "t1": t1, "t2": t2, "c": 1.0},
            {"type": "B", "id": "b2", "attr": "data1", "v": "initb"},
            {"type": "B", "id": "b2", "attr": "data2", "v": "initb"},
            {"type": "A", "id": "a2", "attr": "data1", "v": "inita"},
            {"type": "A", "id": "a2", "attr": "data2", "v": "inita"},
            # For test_hook_dependency_value_forwarding
            {"type": "D", "id": "d1", "attr": "cs", "v": "c1", "t1": t1, "t2": t2, "c": 1.0},
            {"type": "D", "id": "d1", "attr": "data1", "v": "initb"},
            {"type": "D", "id": "d1", "attr": "data2", "v": "initb"},
            {"type": "C", "id": "c1", "attr": "ds", "v": "d1", "t1": t1, "t2": t2, "c": 1.0},
            {"type": "C", "id": "c1", "attr": "data1", "v": "inita"},
            {"type": "C", "id": "c1", "attr": "data2", "v": "inita"},
        ]
        cls.push_datapoints(entity_datapoints)
        cls.get_request("control/make_snapshots")
        sleep(3)

    def test_linked_entities_loaded(self):
        for entity, eid in [("A", "a1"), ("B", "b1")]:
            data = self.get_entity_data(f"entity/{entity}/{eid}", EntityEidData)
            for snapshot in data.snapshots:
                self.assertEqual(snapshot["data1"], "initb")
                self.assertEqual(snapshot["data2"], "inita")

    def test_weak_component_detection(self):
        """
        Remove link from the first entity to be processed, so that an elementary BFS starting
        from this entity is not enough to load the entire weakly connected component.
        """
        for entity, eid in [("A", "a2"), ("B", "b2")]:
            data = self.get_entity_data(f"entity/{entity}/{eid}", EntityEidData)
            for snapshot in data.snapshots:
                self.assertEqual(snapshot["data1"], "initb")
                self.assertEqual(snapshot["data2"], "inita")

    def test_hook_dependency_value_forwarding(self):
        for entity, eid in [("D", "d1"), ("C", "c1")]:
            data = self.get_entity_data(f"entity/{entity}/{eid}", EntityEidData)
            for snapshot in data.snapshots:
                self.assertEqual(snapshot["data1"], "modifd")
                self.assertEqual(snapshot["data2"], "modifc")
