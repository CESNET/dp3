import datetime
import json
import sys

import common

from dp3.api.internal.entity_response_models import EntityRawDataPage
from dp3.common.types import UTC


class RawDatapointsIntegration(common.APITest):
    raw_src = "raw-debug@test"

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        now = datetime.datetime.now(UTC)
        first_t1 = (now - datetime.timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4]
        first_t2 = (now - datetime.timedelta(minutes=9)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4]
        second_t1 = (now - datetime.timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4]
        second_t2 = (now - datetime.timedelta(minutes=4)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4]

        datapoints = [
            {"type": "A", "id": 9101, "attr": "data1", "v": "plain-1", "src": cls.raw_src},
            {"type": "A", "id": 9102, "attr": "data1", "v": "plain-2", "src": cls.raw_src},
            {
                "type": "A",
                "id": 9101,
                "attr": "as",
                "v": {"eid": 9102},
                "t1": first_t1,
                "t2": first_t2,
                "c": 1.0,
                "src": cls.raw_src,
            },
            {
                "type": "A",
                "id": 9101,
                "attr": "as",
                "v": {"eid": 9103},
                "t1": second_t1,
                "t2": second_t2,
                "c": 1.0,
                "src": cls.raw_src,
            },
        ]

        response = cls.push_datapoints(datapoints)
        if response.status_code != 200:
            print(json.dumps(response.json(), indent=2), file=sys.stderr)
            raise Exception(f"Failed to push datapoints: {response.status_code}")

    def test_get_raw_datapoints(self):
        payload = self.query_expected_value(
            lambda: self.get_request(
                "entity/A/raw/get",
                eid=9101,
                attr="as",
                src=self.raw_src,
                limit=2,
            ).json(),
            lambda data: data["count"] == 2,
            msg="Timed out waiting for raw datapoints to appear.",
        )
        page = EntityRawDataPage.model_validate(payload)
        self.assertEqual(2, page.count)
        self.assertEqual([9103, 9102], [item["v"]["eid"] for item in page.data])
        self.assertEqual(["A", "A"], [item["type"] for item in page.data])
        self.assertEqual([9101, 9101], [item["id"] for item in page.data])
        self.assertNotIn("etype", page.data[0])
        self.assertNotIn("eid", page.data[0])

    def test_get_plain_raw_datapoints_newest_first(self):
        payload = self.query_expected_value(
            lambda: self.get_request(
                "entity/A/raw/get",
                attr="data1",
                src=self.raw_src,
                limit=2,
            ).json(),
            lambda data: data["count"] == 2,
            msg="Timed out waiting for plain raw datapoints to appear.",
        )
        page = EntityRawDataPage.model_validate(payload)
        self.assertEqual(["plain-2", "plain-1"], [item["v"] for item in page.data])
        self.assertEqual([9102, 9101], [item["id"] for item in page.data])
