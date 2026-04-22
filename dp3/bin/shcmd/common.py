#!/usr/bin/env python3
"""Shared helpers for the shell-oriented DP3 CLI."""

import json
import os
import sys
from functools import lru_cache
from typing import Any, Optional
from urllib.parse import urljoin

import requests

from dp3.common.config import ModelSpec, read_config_dir


class APIError(RuntimeError):
    """Raised when an API request fails."""


JSON_LITERAL_HELP = (
    "'set' requires a JSON literal value, for example '\"hello\"', '42', 'true', or '{\"k\":1}'."
)


class DP3APIClient:
    """Small HTTP client for the DP3 API."""

    def __init__(
        self,
        config_dir: str,
        base_url: Optional[str] = None,
        timeout: float = 5.0,
        model_spec: Optional[ModelSpec] = None,
    ):
        self.config_dir = os.path.abspath(config_dir)
        self.model_spec = model_spec
        self.base_url = self._resolve_base_url(base_url)
        self.timeout = timeout
        self.session = requests.Session()

    @staticmethod
    def _normalize_base_url(base_url: str) -> str:
        return base_url.rstrip("/") + "/"

    def _resolve_base_url(self, base_url: Optional[str]) -> str:
        if base_url is not None:
            normalized = self._normalize_base_url(base_url)
            self._check_health(normalized)
            return normalized

        candidates = [
            "http://127.0.0.1:8081",
            "http://localhost:8081",
            "http://127.0.0.1:8081/api",
            "http://localhost:8081/api",
            "http://127.0.0.1:5000",
            "http://localhost:5000",
            "http://127.0.0.1:5000/api",
            "http://localhost:5000/api",
        ]
        errors = []
        for candidate in candidates:
            normalized = self._normalize_base_url(candidate)
            try:
                self._check_health(normalized)
                return normalized
            except APIError as e:
                errors.append(f"- {candidate}: {e}")

        joined_errors = "\n".join(errors)
        raise APIError(f"Unable to reach a DP3 API. Tried:\n{joined_errors}")

    def _check_health(self, base_url: str) -> None:
        try:
            response = requests.get(urljoin(base_url, ""), timeout=2)
        except requests.RequestException as e:
            raise APIError(str(e)) from e

        if response.status_code != 200:
            raise APIError(f"unexpected status {response.status_code}")

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[dict[str, Any]] = None,
        json_body: Any = None,
        stream: bool = False,
    ) -> requests.Response:
        url = urljoin(self.base_url, path.lstrip("/"))
        try:
            response = self.session.request(
                method,
                url,
                params=params,
                json=json_body,
                timeout=self.timeout,
                stream=stream,
            )
        except requests.RequestException as e:
            raise APIError(str(e)) from e

        if response.status_code >= 400:
            detail = response.text.strip() or f"HTTP {response.status_code}"
            raise APIError(detail)
        return response


def read_json_value(raw_value: str) -> Any:
    """Decode a JSON literal from a command-line argument."""
    try:
        return json.loads(raw_value)
    except json.JSONDecodeError as e:
        raise APIError(f"Invalid JSON value: {e}") from e


def read_json_input(path: Optional[str]) -> Any:
    """Decode JSON from a file path or standard input."""
    if path in (None, "-"):
        content = sys.stdin.read()
    else:
        with open(path, encoding="utf-8") as file_handle:
            content = file_handle.read()
    try:
        return json.loads(content)
    except json.JSONDecodeError as e:
        raise APIError(f"Invalid JSON input: {e}") from e


def print_response_json(response: requests.Response) -> int:
    """Write an API JSON response to standard output."""
    sys.stdout.write(response.text)
    if response.text and not response.text.endswith("\n"):
        sys.stdout.write("\n")
    return 0


def common_time_params(args) -> dict[str, Any]:
    """Build shared time-range query parameters from parsed arguments."""
    params = {}
    if getattr(args, "date_from", None) is not None:
        params["date_from"] = args.date_from
    if getattr(args, "date_to", None) is not None:
        params["date_to"] = args.date_to
    return params


def read_json_object(raw_value: str, flag_name: str) -> dict[str, Any]:
    """Decode a JSON object from a command-line argument."""
    value = read_json_value(raw_value)
    if not isinstance(value, dict):
        raise APIError(f"{flag_name} must decode to a JSON object.")
    return value


def _extract_page_items(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("data"), list):
        return payload["data"]
    raise APIError("Expected a list response or an object containing a 'data' list.")


def stream_json_pages(
    client: DP3APIClient,
    path: str,
    params: dict[str, Any],
    start_skip: int,
    requested_limit: int,
    page_size: int = 100,
) -> int:
    """Stream paged JSON API results as NDJSON."""
    emitted = 0
    skip = start_skip
    remaining = requested_limit

    try:
        while True:
            batch_limit = page_size if requested_limit == 0 else min(page_size, remaining)
            page_params = dict(params)
            page_params["skip"] = skip
            page_params["limit"] = batch_limit
            response = client.request("GET", path, params=page_params)
            items = _extract_page_items(response.json())
            if not items:
                break

            for item in items:
                sys.stdout.write(json.dumps(item))
                sys.stdout.write("\n")
                sys.stdout.flush()
                emitted += 1
                if requested_limit != 0 and emitted >= requested_limit:
                    return 0

            fetched = len(items)
            skip += fetched
            if requested_limit != 0:
                remaining -= fetched
                if remaining <= 0:
                    break
            if fetched < batch_limit:
                break
    except BrokenPipeError:
        return 0
    return 0


def resolve_config_dir(config_dir: Optional[str]) -> str:
    """Resolve the configuration directory for the shell-oriented CLI."""
    if config_dir is not None:
        return os.path.abspath(config_dir)
    if os.environ.get("DP3_CONFIG_DIR"):
        return os.path.abspath(os.environ["DP3_CONFIG_DIR"])
    return os.path.abspath("config")


@lru_cache(maxsize=32)
def load_completion_model_spec(config_dir: str) -> Optional[ModelSpec]:
    """Load the model specification used by shell completion."""
    try:
        config = read_config_dir(config_dir, recursive=True)
        return ModelSpec(config.get("db_entities"))
    except Exception:
        return None


@lru_cache(maxsize=32)
def load_completion_entity_catalog(
    config_dir: str, base_url: Optional[str], timeout: float
) -> Optional[dict[str, Any]]:
    """Load entity metadata from the API when config-based completion is unavailable."""
    try:
        client = DP3APIClient(config_dir, base_url, timeout)
        payload = client.request("GET", "/entities").json()
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def get_completion_context(
    parsed_args,
) -> tuple[Optional[ModelSpec], Optional[dict[str, Any]]]:
    """Return completion metadata derived from config and API sources."""
    config_dir = resolve_config_dir(getattr(parsed_args, "config", None))
    model_spec = load_completion_model_spec(config_dir)
    entity_catalog = None
    if model_spec is None:
        entity_catalog = load_completion_entity_catalog(
            config_dir,
            getattr(parsed_args, "url", None),
            getattr(parsed_args, "timeout", 5.0),
        )
    return model_spec, entity_catalog


def _entity_type_description(
    etype: str,
    model_spec: Optional[ModelSpec],
    entity_catalog: Optional[dict[str, Any]],
) -> str:
    if model_spec is not None and etype in model_spec.entities:
        entity_spec = model_spec.entity(etype)
        return f"{entity_spec.name} ({entity_spec.id_data_type.root} ids)"
    if entity_catalog is not None and etype in entity_catalog:
        entry = entity_catalog[etype]
        name = entry.get("name")
        id_data_type = entry.get("id_data_type")
        if name and id_data_type:
            return f"{name} ({id_data_type} ids)"
        if name:
            return str(name)
    return "Configured entity type."


def complete_entity_type_names(prefix: str, parsed_args, **_kwargs) -> dict[str, str]:
    """Complete entity type names from config or API metadata."""
    model_spec, entity_catalog = get_completion_context(parsed_args)
    if model_spec is not None:
        values = sorted(model_spec.entities)
    elif entity_catalog is not None:
        values = sorted(entity_catalog)
    else:
        values = []
    return {
        value: _entity_type_description(value, model_spec, entity_catalog)
        for value in values
        if value.startswith(prefix)
    }
