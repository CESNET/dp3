import datetime
import json
import sys
from time import sleep

import common

from dp3.api.internal.entity_response_models import EntityEidData


class SnapshotIntegration(common.APITest):
    @classmethod
    def setUpClass(cls) -> None:
        now = datetime.datetime.now()
        t1 = (now - datetime.timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4]
        t2 = (now + datetime.timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4]

        def make_dp(type, id, attr, v, time=False):
            base = {"type": type, "id": id, "attr": attr, "v": v}
            if time:
                base |= {"t1": t1, "t2": t2, "c": 1.0}
            return base

        entity_datapoints = [
            # For test_linked_entities_loaded (B-"b1" <-> A-420)
            make_dp("B", "b1", "as", {"eid": 420}, time=True),
            make_dp("B", "b1", "data1", "initb"),
            make_dp("B", "b1", "data2", "initb"),
            make_dp("A", 420, "bs", {"eid": "b1"}, time=True),
            make_dp("A", 420, "data1", "inita"),
            make_dp("A", 420, "data2", "inita"),
            # For test_weak_component_detection (B-"b2" -> A-422)
            make_dp("B", "b2", "as", {"eid": 422}, time=True),
            make_dp("B", "b2", "data1", "initb"),
            make_dp("B", "b2", "data2", "initb"),
            make_dp("A", 422, "data1", "inita"),
            make_dp("A", 422, "data2", "inita"),
            # For test_hook_dependency_value_forwarding (D-"d1" <-> C-"c1")
            make_dp("D", "d1", "cs", {"eid": "c1"}, time=True),
            make_dp("D", "d1", "data1", "initb"),
            make_dp("D", "d1", "data2", "initb"),
            make_dp("C", "c1", "ds", {"eid": "d1"}, time=True),
            make_dp("C", "c1", "data1", "inita"),
            make_dp("C", "c1", "data2", "inita"),
        ]
        res = cls.push_datapoints(entity_datapoints)
        if res.status_code != 200:
            print(json.dumps(res.json(), indent=2), sys.stderr)
            raise Exception(f"Failed to push datapoints: {res.status_code}")

        sleep(3)
        cls.get_request("control/make_snapshots")
        sleep(3)

    def test_linked_entities_loaded(self):
        for entity, eid in [("A", 420), ("B", "b1")]:
            data = self.get_entity_data(f"entity/{entity}/{eid}", EntityEidData)
            self.assertGreater(len(data.snapshots), 0)
            for snapshot in data.snapshots:
                self.assertEqual(snapshot["data1"], "initb")
                self.assertEqual(snapshot["data2"], "inita")

    def test_weak_component_detection(self):
        """
        Remove link from the first entity to be processed, so that an elementary BFS starting
        from this entity is not enough to load the entire weakly connected component.
        """
        for entity, eid in [("B", "b2")]:
            data = self.get_entity_data(f"entity/{entity}/{eid}", EntityEidData)
            self.assertGreater(len(data.snapshots), 0)
            for snapshot in data.snapshots:
                self.assertEqual(snapshot["data1"], "initb")
                self.assertEqual(snapshot["data2"], "inita")

    def test_hook_dependency_value_forwarding(self):
        for entity, eid in [("D", "d1"), ("C", "c1")]:
            data = self.get_entity_data(f"entity/{entity}/{eid}", EntityEidData)
            self.assertGreater(len(data.snapshots), 0)
            for snapshot in data.snapshots:
                self.assertEqual(snapshot["data1"], "modifd")
                self.assertEqual(snapshot["data2"], "modifc")
