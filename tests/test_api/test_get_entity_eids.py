import sys
from time import sleep

import common

from api.internal.entity_response_models import EntityEidList


class GetEntityEids(common.APITest):
    snapshot_ready = False

    def setUp(self) -> None:
        super().setUp()
        if not self.snapshot_ready:
            dp_base = {"src": "setup@test", "attr": "data1", "type": "A"}
            for i in range(0, 100, 20):
                res = self.push_datapoints(
                    [{**dp_base, "id": f"A{i}", "v": f"v{i}"} for i in range(i, i + 20)]
                )
                print(res.content.decode("utf-8"), file=sys.stderr)
            sleep(60)
        self.snapshot_ready = True

    def test_get_entity_eids(self):
        eids = self.get_entity_data("entity/A", EntityEidList)
        self.assertEqual(20, len(eids.data))

    def test_get_entity_eids_pagination(self):
        expected_eids = {f"A{i}" for i in range(0, 100)}
        received_eids = set()

        for i in range(0, 100, 10):
            eids = self.get_entity_data("entity/A", EntityEidList, skip=i, limit=10)
            self.assertEqual(10, len(eids.data))
            received_eids.update(x["eid"] for x in eids.data)

        eids = self.get_entity_data("entity/A", EntityEidList, skip=101, limit=20)
        self.assertEqual(0, len(eids.data))
        self.assertSetEqual(expected_eids, received_eids)
