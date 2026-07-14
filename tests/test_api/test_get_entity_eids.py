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
        eids = self.get_entity_data("entity/A/get", EntityEidList)
        self.assertEqual(20, len(eids.data))

    def test_get_entity_eids_pagination(self):
        expected_eids = set(range(0, 100))
        received_eids = set()

        for i in range(0, 100, 10):
            eids = self.get_entity_data("entity/A/get", EntityEidList, skip=i, limit=10)
            self.assertEqual(10, len(eids.data), f"Failed at {i}")
            received_eids.update(x["eid"] for x in eids.data)

        eids = self.get_entity_data("entity/A/get", EntityEidList, skip=102, limit=20)
        self.assertEqual(0, len(eids.data))
        self.assertSetEqual(expected_eids, received_eids)

    def test_get_entity_eids_generic_filter(self):
        eids = self.get_entity_data(
            "entity/A/get", EntityEidList, generic_filter=json.dumps({"last.eid": 0})
        )
        self.assertEqual(1, len(eids.data))
        self.assertEqual(0, eids.data[0]["eid"])

    def test_get_entity_eids_generic_filters_eid(self):
        eids = self.get_entity_data(
            "entity/A/get",
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

    def test_get_entity_eids_sort_by_eid_asc(self):
        eids = self.get_entity_data("entity/A/get", EntityEidList, sort="eid:1", limit=0)
        self.assertEqual(100, len(eids.data))
        self.assertEqual(list(range(100)), [x["eid"] for x in eids.data])

    def test_get_entity_eids_sort_by_eid_desc(self):
        eids = self.get_entity_data("entity/A/get", EntityEidList, sort="eid:-1", limit=0)
        self.assertEqual(100, len(eids.data))
        self.assertEqual(list(range(99, -1, -1)), [x["eid"] for x in eids.data])

    def test_get_entity_eids_pagination_with_sort(self):
        received_eids = []
        for i in range(0, 100, 10):
            eids = self.get_entity_data(
                "entity/A/get", EntityEidList, sort="eid:1", skip=i, limit=10
            )
            self.assertEqual(10, len(eids.data), f"Failed at {i}")
            received_eids.extend(x["eid"] for x in eids.data)
        self.assertEqual(list(range(100)), received_eids)

        received_eids = []
        for i in range(0, 100, 10):
            eids = self.get_entity_data(
                "entity/A/get", EntityEidList, sort="eid:-1", skip=i, limit=10
            )
            self.assertEqual(10, len(eids.data), f"Failed at {i}")
            received_eids.extend(x["eid"] for x in eids.data)
        self.assertEqual(list(range(99, -1, -1)), received_eids)

    def _get_sorted_eids(self, sort_params: list[str], **kwargs) -> EntityEidList:
        """Fetch entity EIDs with multiple sort query parameters.

        The shared `get_request` helper joins kwargs with '&' and cannot emit
        multiple values for the same key, which is required for multi-column
        sorting. Build the query string explicitly here.
        """
        query_parts = [f"sort={param}" for param in sort_params]
        for key, value in kwargs.items():
            query_parts.append(f"{key}={value}")
        response = self.get_request(f"entity/A/get?{'&'.join(query_parts)}")
        self.assertEqual(response.status_code, 200)
        return EntityEidList.model_validate_json(response.content)

    def test_get_entity_eids_sort_by_multiple_attrs(self):
        res = self.push_datapoints(
            [
                {"src": "setup@test", "attr": "data2", "type": "A", "id": i, "v": f"g{i % 5}"}
                for i in range(0, 100)
            ]
        )
        self.assertEqual(res.status_code, 200)
        sleep(8)
        self.get_request("control/make_snapshots")
        sleep(6)

        expected = sorted(range(100), key=lambda i: (f"g{i % 5}", i))
        # omit :1 if sort is ascending
        eids = self._get_sorted_eids(["data2", "eid"], limit=0)
        self.assertEqual(100, len(eids.data))
        self.assertEqual(expected, [x["eid"] for x in eids.data])

        expected = sorted(range(100), key=lambda i: (f"g{i % 5}", -i))
        eids = self._get_sorted_eids(["data2:1", "eid:-1"], limit=0)
        self.assertEqual(100, len(eids.data))
        self.assertEqual(expected, [x["eid"] for x in eids.data])
