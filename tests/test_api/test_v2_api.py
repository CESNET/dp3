"""Integration tests for v2 API endpoints with query-parameter EIDs."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import common
import requests


class V2APIIntegration(common.APITest):
    """Verify that v2 routes preserve EIDs containing path separators."""

    etype = "test_entity_type"
    attr = "test_attr_string"

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.eid = f"v2/path/{uuid4()}"
        cls.initial_value = "v2 initial value"

        response = cls.push_datapoints(
            [
                {
                    "type": cls.etype,
                    "id": cls.eid,
                    "attr": cls.attr,
                    "v": cls.initial_value,
                    "src": "v2-api-test",
                }
            ]
        )
        if response.status_code != 200:
            raise RuntimeError(f"Failed to push v2 API test datapoint: {response.text}")

    @classmethod
    def v2_url(cls, suffix: str = "") -> str:
        return f"{common.base_url}/entity/v2/{cls.etype}{suffix}"

    @classmethod
    def get_v2(cls, suffix: str = "", **params) -> requests.Response:
        return common.retry_request_on_error(
            lambda: requests.get(cls.v2_url(suffix), params=params, timeout=5)
        )

    @classmethod
    def post_v2(cls, suffix: str, json, **params) -> requests.Response:
        return common.retry_request_on_error(
            lambda: requests.post(cls.v2_url(suffix), params=params, json=json, timeout=5)
        )

    @classmethod
    def delete_v2(cls, suffix: str = "", **params) -> requests.Response:
        return common.retry_request_on_error(
            lambda: requests.delete(cls.v2_url(suffix), params=params, timeout=5)
        )

    def test_01_get_entity_data_with_trailing_slash(self):
        response = self.query_expected_value(
            lambda: self.get_v2("/", eid=self.eid),
            lambda result: result.status_code == 200
            and result.json()["master_record"].get(self.attr, {}).get("v") == self.initial_value,
            msg="Timed out waiting for the v2 entity data endpoint.",
        )
        self.assertEqual(response.json()["master_record"][self.attr]["v"], self.initial_value)

    def test_02_get_entity_data_without_trailing_slash(self):
        response = self.get_v2(eid=self.eid)
        self.assertEqual(response.status_code, 200, msg=response.text)
        self.assertEqual(response.json()["master_record"][self.attr]["v"], self.initial_value)

    def test_03_get_master_record(self):
        response = self.get_v2("/master", eid=self.eid)
        self.assertEqual(response.status_code, 200, msg=response.text)
        self.assertEqual(response.json()[self.attr]["v"], self.initial_value)

    def test_04_get_snapshots(self):
        response = self.get_v2("/snapshots", eid=self.eid)
        self.assertEqual(response.status_code, 200, msg=response.text)
        self.assertIsInstance(response.json(), list)

    def test_05_get_attribute(self):
        response = self.get_v2("/attr", eid=self.eid, attr=self.attr)
        self.assertEqual(response.status_code, 200, msg=response.text)
        self.assertEqual(response.json()["current_value"], self.initial_value)

    def test_06_set_attribute(self):
        updated_value = "v2 updated value"
        response = self.post_v2("/attr", {"value": updated_value}, eid=self.eid, attr=self.attr)
        self.assertEqual(response.status_code, 200, msg=response.text)

        self.query_expected_value(
            lambda: self.get_v2("/attr", eid=self.eid, attr=self.attr),
            lambda result: result.status_code == 200
            and result.json().get("current_value") == updated_value,
            msg="Timed out waiting for the v2 attribute update.",
        )

    def test_07_extend_ttl(self):
        future_time = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
        response = self.post_v2("/ttl", {"manual": future_time}, eid=self.eid)
        self.assertEqual(response.status_code, 200, msg=response.text)

    def test_08_missing_eid_returns_422(self):
        response = self.get_v2("/master")
        self.assertEqual(response.status_code, 422, msg=response.text)

    def test_09_nonexistent_etype_returns_422(self):
        response = common.retry_request_on_error(
            lambda: requests.get(
                f"{common.base_url}/entity/v2/nonexistent_type/master",
                params={"eid": self.eid},
                timeout=5,
            )
        )
        self.assertEqual(response.status_code, 422, msg=response.text)

    def test_99_delete_entity_with_and_without_trailing_slash(self):
        response = self.delete_v2(eid=self.eid)
        self.assertEqual(response.status_code, 200, msg=response.text)

        response = self.delete_v2("/", eid=self.eid)
        self.assertEqual(response.status_code, 200, msg=response.text)
