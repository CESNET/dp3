"""Tests for v2 API endpoints with query-parameter EIDs.

These tests verify that the v2 API correctly handles EIDs containing special
characters like '/' (e.g., IPv6 CIDR notation: 2001:db8:f00::/64).
"""

from datetime import UTC

import pytest
from fastapi.testclient import TestClient

from dp3.api.main import app


@pytest.fixture
def client():
    """Create a test client for the DP3 API."""
    return TestClient(app, raise_server_exceptions=False)


class TestV2APIBasicEndpoints:
    """Test basic v2 API endpoints that don't require EID in path."""

    def test_v2_get_entity_type_eids(self, client):
        """Test v2 GET /entity/v2/{etype}/get endpoint."""
        # This will return empty data or error if etype doesn't exist,
        # but verifies the route is registered and accessible
        response = client.get("/entity/v2/example/get")
        # Should not be 404 (route not found)
        assert response.status_code != 404

    def test_v2_count_entity_type_eids(self, client):
        """Test v2 GET /entity/v2/{etype}/count endpoint."""
        response = client.get("/entity/v2/example/count")
        assert response.status_code != 404

    def test_v2_get_distinct_attribute_values(self, client):
        """Test v2 GET /entity/v2/{etype}/_/distinct/{attr} endpoint."""
        response = client.get("/entity/v2/example/_/distinct/some_attr")
        assert response.status_code != 404


class TestV2APIWithSpecialCharacterEIDs:
    """Test v2 API endpoints with EIDs containing special characters."""

    def test_v2_snapshots_with_ipv6_cidr(self, client):
        """Test v2 snapshots endpoint with IPv6 CIDR notation EID.

        This is the primary use case that motivated the v2 API - EIDs
        containing '/' should work when passed as query parameter.
        """
        ipv6_cidr = "2001:db8:f00::/64"
        response = client.get(f"/entity/v2/ipv6_64prefix/snapshots?eid={ipv6_cidr}")
        # Should not be 404 - the route should match
        # Response may be 422 (validation) or 400 (no data) or 200 (success)
        assert response.status_code != 404

    def test_v2_master_with_ipv6_cidr(self, client):
        """Test v2 master endpoint with IPv6 CIDR notation EID."""
        ipv6_cidr = "2001:db8:f00::/64"
        response = client.get(f"/entity/v2/ipv6_64prefix/master?eid={ipv6_cidr}")
        assert response.status_code != 404

    def test_v2_get_with_path_like_eid(self, client):
        """Test v2 get endpoint with path-like EID (e.g., 'foo/bar/baz')."""
        path_eid = "foo/bar/baz"
        response = client.get(f"/entity/v2/some_type/?eid={path_eid}")
        assert response.status_code != 404

    def test_v2_attr_get_with_special_eid(self, client):
        """Test v2 attribute get endpoint with special character EID."""
        special_eid = "user/name"
        response = client.get(f"/entity/v2/user/attr?eid={special_eid}&attr=hostname")
        assert response.status_code != 404


class TestV2APIValidation:
    """Test v2 API validation behavior."""

    def test_v2_missing_eid_param_returns_422(self, client):
        """Test that missing EID query parameter returns 422 Unprocessable Entity."""
        response = client.get("/entity/v2/example/master")
        assert response.status_code == 422

    def test_v2_invalid_eid_format_returns_422(self, client):
        """Test that invalid EID format returns appropriate error."""
        # Invalid EID depending on entity type's expected format
        response = client.get("/entity/v2/example/master?eid=")
        # Empty string may be rejected by validation
        assert response.status_code in (422, 400)

    def test_v2_nonexistent_etype_returns_422(self, client):
        """Test that nonexistent entity type returns 422."""
        response = client.get("/entity/v2/nonexistent_type/master?eid=some_id")
        assert response.status_code == 422


class TestV2APIPostEndpoints:
    """Test v2 POST endpoints."""

    def test_v2_set_attr_value(self, client):
        """Test v2 set attribute value endpoint."""
        response = client.post(
            "/entity/v2/example/attr?eid=test_id&attr=hostname", json={"value": "new_hostname"}
        )
        # Should not be 404 - route should match
        assert response.status_code != 404

    def test_v2_extend_ttl(self, client):
        """Test v2 extend TTL endpoint."""
        from datetime import datetime, timedelta

        future_time = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
        response = client.post("/entity/v2/example/ttl?eid=test_id", json={"default": future_time})
        assert response.status_code != 404


class TestV2APIDeleteEndpoint:
    """Test v2 DELETE endpoint."""

    def test_v2_delete_entity(self, client):
        """Test v2 delete entity endpoint."""
        response = client.delete("/entity/v2/example/?eid=test_id")
        # Should not be 404 - route should match
        assert response.status_code != 404
