import sys
from datetime import datetime, timedelta

import common
from pydantic import RootModel

from dp3.api.internal.entity_response_models import EntityEidData, EntityEidMasterRecord

DATAPOINT_COUNT = 6


class GetEntityEidData(common.APITest):
    eid = None

    def setUp(self) -> None:
        super().setUpClass()
        t1 = datetime.now() - timedelta(minutes=30)
        t2 = t1 + timedelta(minutes=10)
        self.eid = f"test_get_data__{datetime.now()}"
        dp_base = {
            "src": "setup@test",
            "attr": "test_attr_history",
            "type": "test_entity_type",
            "id": self.eid,
        }
        dps = []
        for i in range(DATAPOINT_COUNT):
            dps.append(
                {
                    **dp_base,
                    "t1": t1.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4],
                    "t2": t2.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4],
                    "v": i,
                }
            )
            t1 += timedelta(minutes=10)
            t2 += timedelta(minutes=10)
        print(dps, file=sys.stderr)
        res = self.push_datapoints(dps)
        print(res.content.decode("utf-8"), file=sys.stderr)
        if res.status_code != 200:
            raise Exception(f"Failed to push datapoints: {res.status_code}")

    def test_get_entity_data(self):
        data = self.query_expected_value(
            lambda: self.get_entity_data(f"entity/test_entity_type/{self.eid}", EntityEidData),
            lambda data: "test_attr_history" in data.master_record,
        )
        self.assertEqual(DATAPOINT_COUNT, len(data.master_record["test_attr_history"]))

    def test_get_entity_master_record(self):
        data = self.query_expected_value(
            lambda: self.get_entity_data(
                f"entity/test_entity_type/{self.eid}/master", RootModel[EntityEidMasterRecord]
            ),
            lambda data: "test_attr_history" in data.root,
        )
        self.assertEqual(DATAPOINT_COUNT, len(data.root["test_attr_history"]))
