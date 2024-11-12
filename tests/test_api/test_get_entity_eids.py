import json
import sys
from time import sleep

import common

from dp3.api.internal.entity_response_models import EntityEidList


class GetEntityEids(common.APITest):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        dp_base = {"src": "setup@test", "attr": "data1", "type": "A"}
        for i in range(0, 100, 20):
            res = cls.push_datapoints(
                [{**dp_base, "id": i, "v": f"v{i}"} for i in range(i, i + 20)]
            )
            if res.status_code != 200:
                print(json.dumps(res.json(), indent=2), file=sys.stderr)
                raise Exception(f"Failed to push datapoints: {res.status_code}")
        sleep(8)
        cls.get_request("control/make_snapshots")
        sleep(6)

    def test_get_entity_eids(self):
        eids = self.get_entity_data("entity/A", EntityEidList)
        self.assertEqual(20, len(eids.data))

    def test_get_entity_eids_pagination(self):
        expected_eids = set(range(0, 100))
        received_eids = set()

        for i in range(0, 100, 10):
            eids = self.get_entity_data("entity/A", EntityEidList, skip=i, limit=10)
            self.assertEqual(10, len(eids.data), f"Failed at {i}")
            received_eids.update(x["eid"] for x in eids.data)

        eids = self.get_entity_data("entity/A", EntityEidList, skip=102, limit=20)
        self.assertEqual(0, len(eids.data))
        self.assertSetEqual(expected_eids, received_eids)

    def test_get_entity_eids_generic_filter(self):
        eids = self.get_entity_data(
            "entity/A", EntityEidList, generic_filter=json.dumps({"last.eid": 0})
        )
        self.assertEqual(1, len(eids.data))
        self.assertEqual(0, eids.data[0]["eid"])

    def test_get_entity_eids_generic_filters_eid(self):
        eids = self.get_entity_data(
            "entity/A",
            EntityEidList,
            generic_filter=json.dumps(
                {"$or": [{"last.eid": 5}, {"last.eid": {"$gte": 50, "$lt": 60}}]}
            ),
        )
        self.assertEqual(11, len(eids.data))  # A5, A50 ... A59
        self.assertEqual(
            {5, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59},
            {x["eid"] for x in eids.data},
        )
