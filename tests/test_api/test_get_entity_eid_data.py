import sys
from datetime import datetime, timedelta

import common

from api.internal.entity_response_models import EntityEidData


class GetEntityEidData(common.APITest):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        t1 = datetime.now() - timedelta(minutes=30)
        t2 = t1 + timedelta(minutes=10)
        dp_base = {
            "src": "setup@test",
            "attr": "test_attr_history",
            "type": "test_entity_type",
            "id": "test_get_data",
        }
        dps = []
        for i in range(6):
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
        res = cls.push_datapoints(dps)
        print(res.content.decode("utf-8"), file=sys.stderr)

    def test_get_entity_data(self):
        data = self.get_entity_data("entity/test_entity_type/test_get_data", EntityEidData)
        self.assertIn("test_attr_history", data.master_record)
        self.assertEqual(6, len(data.master_record["test_attr_history"]))
