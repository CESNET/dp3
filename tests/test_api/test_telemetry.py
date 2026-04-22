import datetime
import json
import sys
from time import sleep

import common

from dp3.common.types import UTC


class TelemetryEndpoints(common.APITest):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        now = datetime.datetime.now(UTC)
        t1 = (now - datetime.timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4]
        t2 = (now + datetime.timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4]

        datapoints = [
            {"type": "A", "id": 999, "attr": "data1", "v": "telemetry-test"},
            {
                "type": "A",
                "id": 999,
                "attr": "bs",
                "v": {"eid": "telemetry-b"},
                "t1": t1,
                "t2": t2,
                "c": 1.0,
                "src": "telemetry-test",
            },
        ]
        res = cls.push_datapoints(datapoints)
        if res.status_code != 200:
            print(json.dumps(res.json(), indent=2), sys.stderr)
            raise Exception(f"Failed to push datapoints: {res.status_code}")

        sleep(3)
        cls.get_request("control/make_snapshots")
        sleep(3)

    def test_snapshot_summary_endpoint(self):
        response = self.get_request("telemetry/snapshot_summary")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsNotNone(data["latest_age"])

    def test_metadata_endpoint(self):
        response = self.get_request("telemetry/metadata", module="SnapShooter", limit=1)
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["#module"], "SnapShooter")

    def test_entities_per_attr_endpoint(self):
        response = self.get_request("telemetry/entities_per_attr")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("A", data)
        self.assertIn("data1", data["A"])
        self.assertGreaterEqual(data["A"]["data1"], 1)
