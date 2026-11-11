import io
import unittest
from argparse import Namespace
from contextlib import redirect_stdout
from unittest.mock import MagicMock

from dp3.bin.shcmd.entity import attr, etype, instance
from dp3.bin.shcmd.entity.common import handle_attr_set_request, handle_raw

EID = "2001:db8:f00::/64"


class TestShEntityRequests(unittest.TestCase):
    """The entity commands use the v2 API, which takes the entity id as a query parameter."""

    def _run(self, handler, **args) -> MagicMock:
        client = MagicMock()
        client.request.return_value.text = "{}"
        with redirect_stdout(io.StringIO()):
            self.assertEqual(handler(client, Namespace(etype="prefix", **args)), 0)
        return client

    def _assert_request(self, client: MagicMock, method: str, path: str, **kwargs) -> None:
        client.request.assert_called_once_with(method, path, **kwargs)

    def test_get(self):
        client = self._run(instance.handle_get, eid=EID, date_from="2024-01-01T00:00:00Z")
        self._assert_request(
            client,
            "GET",
            "/v2/entity/prefix/",
            params={"eid": EID, "date_from": "2024-01-01T00:00:00Z"},
        )

    def test_master(self):
        client = self._run(instance.handle_master, eid=EID)
        self._assert_request(client, "GET", "/v2/entity/prefix/master", params={"eid": EID})

    def test_snapshots(self):
        client = self._run(instance.handle_snapshots, eid=EID, format="json", skip=5, limit=10)
        self._assert_request(
            client,
            "GET",
            "/v2/entity/prefix/snapshots",
            params={"eid": EID, "skip": 5, "limit": 10},
        )

    def test_snapshots_ndjson(self):
        client = MagicMock()
        client.request.return_value.json.return_value = []
        args = Namespace(etype="prefix", eid=EID, format="ndjson", skip=0, limit=0)
        self.assertEqual(instance.handle_snapshots(client, args), 0)
        self._assert_request(
            client,
            "GET",
            "/v2/entity/prefix/snapshots",
            params={"eid": EID, "skip": 0, "limit": 100},
        )

    def test_ttl(self):
        client = self._run(instance.handle_ttl, eid=EID, body_json='{"manual":"2030-01-01T00:00Z"}')
        self._assert_request(
            client,
            "POST",
            "/v2/entity/prefix/ttl",
            params={"eid": EID},
            json_body={"manual": "2030-01-01T00:00Z"},
        )

    def test_delete(self):
        client = self._run(instance.handle_delete, eid=EID)
        self._assert_request(client, "DELETE", "/v2/entity/prefix/", params={"eid": EID})

    def test_attr_get(self):
        client = self._run(attr.handle_get, eid=EID, attr="name", date_to="2024-02-01T00:00:00Z")
        self._assert_request(
            client,
            "GET",
            "/v2/entity/prefix/attr",
            params={"eid": EID, "attr": "name", "date_to": "2024-02-01T00:00:00Z"},
        )

    def test_attr_set(self):
        client = self._run(handle_attr_set_request, eid=EID, attr="name", value_json='"x"')
        self._assert_request(
            client,
            "POST",
            "/v2/entity/prefix/attr",
            params={"eid": EID, "attr": "name"},
            json_body={"value": "x"},
        )

    def test_raw(self):
        client = self._run(
            handle_raw, eid=EID, attr=None, src="test", format="json", skip=0, limit=20
        )
        self._assert_request(
            client,
            "GET",
            "/v2/entity/prefix/raw/get",
            params={"skip": 0, "limit": 20, "eid": EID, "src": "test"},
        )

    def test_attr_values(self):
        client = self._run(etype.handle_distinct, attr="name")
        self._assert_request(client, "GET", "/v2/entity/prefix/_/distinct/name")


if __name__ == "__main__":
    unittest.main()
