#!/usr/bin/env python3
"""Shell-oriented CLI for interacting with a running DP3 API."""

import argparse
import json
import os
import sys
from typing import Any, Optional
from urllib.parse import urljoin

import requests

from dp3.common.attrspec import AttrType
from dp3.common.config import ModelSpec, read_config_dir


class APIError(RuntimeError):
    """Raised when an API request fails."""


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
        params: dict[str, Any] = None,
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


def _read_json_value(raw_value: str) -> Any:
    try:
        return json.loads(raw_value)
    except json.JSONDecodeError as e:
        raise APIError(f"Invalid JSON value: {e}") from e


def _read_json_input(path: Optional[str]) -> Any:
    if path in (None, "-"):
        content = sys.stdin.read()
    else:
        with open(path, encoding="utf-8") as file_handle:
            content = file_handle.read()
    try:
        return json.loads(content)
    except json.JSONDecodeError as e:
        raise APIError(f"Invalid JSON input: {e}") from e


def _print_response_json(response: requests.Response) -> int:
    sys.stdout.write(response.text)
    if response.text and not response.text.endswith("\n"):
        sys.stdout.write("\n")
    return 0


def _extract_page_items(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("data"), list):
        return payload["data"]
    raise APIError("Expected a list response or an object containing a 'data' list.")


def _stream_json_pages(
    client: DP3APIClient,
    path: str,
    params: dict[str, Any],
    start_skip: int,
    requested_limit: int,
    page_size: int = 100,
) -> int:
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


def _common_time_params(args) -> dict[str, Any]:
    params = {}
    if getattr(args, "date_from", None) is not None:
        params["date_from"] = args.date_from
    if getattr(args, "date_to", None) is not None:
        params["date_to"] = args.date_to
    return params


def _read_json_object(raw_value: str, flag_name: str) -> dict[str, Any]:
    value = _read_json_value(raw_value)
    if not isinstance(value, dict):
        raise APIError(f"{flag_name} must decode to a JSON object.")
    return value


def _build_has_attr_filter(client: DP3APIClient, etype: str, attr: str) -> dict[str, Any]:
    if client.model_spec is None:
        raise APIError("Attribute-presence filtering requires a readable model specification.")
    if etype not in client.model_spec.entities:
        raise APIError(f"Unknown entity type '{etype}'.")
    if attr not in client.model_spec.attribs(etype):
        raise APIError(f"Attribute '{attr}' doesn't exist on entity type '{etype}'.")

    query = {f"last.{attr}": {"$exists": True}}
    attr_spec = client.model_spec.attr(etype, attr)
    if attr_spec.t in AttrType.TIMESERIES | AttrType.OBSERVATIONS:
        query[f"last.{attr}"]["$ne"] = []
    return query


def _entity_generic_filter_param(client: DP3APIClient, args) -> Optional[str]:
    query = None
    if getattr(args, "filter_json", None) is not None:
        query = _read_json_object(args.filter_json, "--filter-json")
    if getattr(args, "has_attr", None) is not None:
        has_attr_filter = _build_has_attr_filter(client, args.etype, args.has_attr)
        query = has_attr_filter if query is None else {"$and": [query, has_attr_filter]}
    if query is None:
        return None
    return json.dumps(query)


def handle_health(client: DP3APIClient, _args) -> int:
    return _print_response_json(client.request("GET", "/"))


def handle_datapoints_ingest(client: DP3APIClient, args) -> int:
    body = _read_json_input(args.path)
    return _print_response_json(client.request("POST", "/datapoints", json_body=body))


def handle_entity_types(client: DP3APIClient, _args) -> int:
    return _print_response_json(client.request("GET", "/entities"))


def handle_entity_list(client: DP3APIClient, args) -> int:
    params = {"skip": args.skip, "limit": args.limit}
    if args.fulltext_json is not None:
        params["fulltext_filters"] = args.fulltext_json
    generic_filter = _entity_generic_filter_param(client, args)
    if generic_filter is not None:
        params["generic_filter"] = generic_filter
    return _print_response_json(client.request("GET", f"/entity/{args.etype}/get", params=params))


def handle_entity_count(client: DP3APIClient, args) -> int:
    params = {}
    if args.fulltext_json is not None:
        params["fulltext_filters"] = args.fulltext_json
    generic_filter = _entity_generic_filter_param(client, args)
    if generic_filter is not None:
        params["generic_filter"] = generic_filter
    return _print_response_json(client.request("GET", f"/entity/{args.etype}/count", params=params))


def handle_entity_raw(client: DP3APIClient, args) -> int:
    params = {"skip": args.skip, "limit": args.limit}
    if args.eid is not None:
        params["eid"] = args.eid
    if args.attr is not None:
        params["attr"] = args.attr
    if args.src is not None:
        params["src"] = args.src
    path = f"/entity/{args.etype}/raw/get"
    if args.format == "ndjson":
        base_params = {key: value for key, value in params.items() if key not in {"skip", "limit"}}
        return _stream_json_pages(client, path, base_params, args.skip, args.limit)
    return _print_response_json(client.request("GET", path, params=params))


def handle_entity_get(client: DP3APIClient, args) -> int:
    return _print_response_json(
        client.request("GET", f"/entity/{args.etype}/{args.eid}", params=_common_time_params(args))
    )


def handle_entity_master(client: DP3APIClient, args) -> int:
    return _print_response_json(
        client.request(
            "GET", f"/entity/{args.etype}/{args.eid}/master", params=_common_time_params(args)
        )
    )


def handle_entity_snapshots(client: DP3APIClient, args) -> int:
    params = _common_time_params(args)
    params["skip"] = args.skip
    params["limit"] = args.limit
    path = f"/entity/{args.etype}/{args.eid}/snapshots"
    if args.format == "ndjson":
        return _stream_json_pages(client, path, _common_time_params(args), args.skip, args.limit)
    return _print_response_json(client.request("GET", path, params=params))


def handle_entity_attr(client: DP3APIClient, args) -> int:
    return _print_response_json(
        client.request(
            "GET",
            f"/entity/{args.etype}/{args.eid}/get/{args.attr}",
            params=_common_time_params(args),
        )
    )


def handle_entity_distinct(client: DP3APIClient, args) -> int:
    return _print_response_json(
        client.request("GET", f"/entity/{args.etype}/_/distinct/{args.attr}")
    )


def handle_entity_set(client: DP3APIClient, args) -> int:
    body = {"value": _read_json_value(args.value_json)}
    return _print_response_json(
        client.request("POST", f"/entity/{args.etype}/{args.eid}/set/{args.attr}", json_body=body)
    )


def handle_entity_ttl(client: DP3APIClient, args) -> int:
    body = _read_json_value(args.body_json)
    return _print_response_json(
        client.request("POST", f"/entity/{args.etype}/{args.eid}/ttl", json_body=body)
    )


def handle_entity_delete(client: DP3APIClient, args) -> int:
    return _print_response_json(client.request("DELETE", f"/entity/{args.etype}/{args.eid}"))


def handle_control_make_snapshots(client: DP3APIClient, _args) -> int:
    return _print_response_json(client.request("GET", "/control/make_snapshots"))


def handle_control_refresh_on_entity_creation(client: DP3APIClient, args) -> int:
    return _print_response_json(
        client.request(
            "GET",
            "/control/refresh_on_entity_creation",
            params={"etype": args.etype},
        )
    )


def handle_control_refresh_module_config(client: DP3APIClient, args) -> int:
    return _print_response_json(
        client.request(
            "GET",
            "/control/refresh_module_config",
            params={"module": args.module},
        )
    )


def handle_telemetry_sources_validity(client: DP3APIClient, _args) -> int:
    return _print_response_json(client.request("GET", "/telemetry/sources_validity"))


def handle_telemetry_source_age(client: DP3APIClient, args) -> int:
    return _print_response_json(
        client.request("GET", "/telemetry/sources_age", params={"unit": args.unit})
    )


def handle_telemetry_entities_per_attr(client: DP3APIClient, _args) -> int:
    return _print_response_json(client.request("GET", "/telemetry/entities_per_attr"))


def handle_telemetry_snapshot_summary(client: DP3APIClient, _args) -> int:
    return _print_response_json(client.request("GET", "/telemetry/snapshot_summary"))


def handle_telemetry_metadata(client: DP3APIClient, args) -> int:
    params = {
        "skip": args.skip,
        "limit": args.limit,
        "sort": args.sort,
    }
    if args.module is not None:
        params["module"] = args.module
    if args.date_from is not None:
        params["date_from"] = args.date_from
    if args.date_to is not None:
        params["date_to"] = args.date_to

    if args.format == "ndjson":
        base_params = {k: v for k, v in params.items() if k not in {"skip", "limit"}}
        return _stream_json_pages(
            client,
            "/telemetry/metadata",
            base_params,
            args.skip,
            args.limit,
        )
    return _print_response_json(client.request("GET", "/telemetry/metadata", params=params))


def handle_telemetry_rabbitmq_queues(client: DP3APIClient, _args) -> int:
    return _print_response_json(client.request("GET", "/telemetry/rabbitmq/queues"))


def init_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--config",
        required=True,
        help="Path to the DP3 configuration directory.",
    )
    parser.add_argument(
        "--url",
        default=None,
        help="Base URL of the DP3 API. When omitted, localhost defaults are probed.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        help="HTTP timeout in seconds.",
    )

    commands = parser.add_subparsers(dest="sh_command", required=True)

    health_parser = commands.add_parser("health", help="Check whether the API is reachable.")
    health_parser.set_defaults(handler=handle_health)

    datapoints_parser = commands.add_parser("datapoints", help="Send datapoints to the API.")
    datapoints_commands = datapoints_parser.add_subparsers(dest="datapoints_command", required=True)
    ingest_parser = datapoints_commands.add_parser(
        "ingest", help="Post datapoints from JSON input."
    )
    ingest_parser.add_argument(
        "path",
        nargs="?",
        default="-",
        help="Path to a JSON file, or '-' / omitted to read stdin.",
    )
    ingest_parser.set_defaults(handler=handle_datapoints_ingest)

    entity_parser = commands.add_parser("entity", help="Inspect and modify entity data.")
    entity_commands = entity_parser.add_subparsers(dest="entity_command", required=True)

    entity_types_parser = entity_commands.add_parser("types", help="List configured entity types.")
    entity_types_parser.set_defaults(handler=handle_entity_types)

    entity_list_parser = entity_commands.add_parser("list", help="List latest entity snapshots.")
    entity_list_parser.add_argument("etype")
    entity_list_parser.add_argument("--fulltext-json", default=None)
    entity_list_parser.add_argument("--filter-json", default=None)
    entity_list_parser.add_argument(
        "--has-attr",
        default=None,
        help="Limit results to latest snapshots where the attribute has data present.",
    )
    entity_list_parser.add_argument("--skip", type=int, default=0)
    entity_list_parser.add_argument("--limit", type=int, default=20)
    entity_list_parser.set_defaults(handler=handle_entity_list)

    entity_count_parser = entity_commands.add_parser("count", help="Count latest entity snapshots.")
    entity_count_parser.add_argument("etype")
    entity_count_parser.add_argument("--fulltext-json", default=None)
    entity_count_parser.add_argument("--filter-json", default=None)
    entity_count_parser.add_argument(
        "--has-attr",
        default=None,
        help="Limit results to latest snapshots where the attribute has data present.",
    )
    entity_count_parser.set_defaults(handler=handle_entity_count)

    entity_raw_parser = entity_commands.add_parser(
        "raw",
        help=(
            "Browse current raw datapoints for troubleshooting ingestion. "
            "Can be slow on large raw collections."
        ),
    )
    entity_raw_parser.add_argument("etype")
    entity_raw_parser.add_argument("--eid")
    entity_raw_parser.add_argument("--attr")
    entity_raw_parser.add_argument("--src")
    entity_raw_parser.add_argument("--skip", type=int, default=0)
    entity_raw_parser.add_argument("--limit", type=int, default=20)
    entity_raw_parser.add_argument("--format", choices=["json", "ndjson"], default="json")
    entity_raw_parser.set_defaults(handler=handle_entity_raw)

    entity_get_parser = entity_commands.add_parser("get", help="Get full entity data.")
    entity_get_parser.add_argument("etype")
    entity_get_parser.add_argument("eid")
    entity_get_parser.add_argument("--from", dest="date_from")
    entity_get_parser.add_argument("--to", dest="date_to")
    entity_get_parser.set_defaults(handler=handle_entity_get)

    entity_master_parser = entity_commands.add_parser("master", help="Get an entity master record.")
    entity_master_parser.add_argument("etype")
    entity_master_parser.add_argument("eid")
    entity_master_parser.add_argument("--from", dest="date_from")
    entity_master_parser.add_argument("--to", dest="date_to")
    entity_master_parser.set_defaults(handler=handle_entity_master)

    entity_snapshots_parser = entity_commands.add_parser(
        "snapshots", help="Get snapshots of a single entity."
    )
    entity_snapshots_parser.add_argument("etype")
    entity_snapshots_parser.add_argument("eid")
    entity_snapshots_parser.add_argument("--from", dest="date_from")
    entity_snapshots_parser.add_argument("--to", dest="date_to")
    entity_snapshots_parser.add_argument("--skip", type=int, default=0)
    entity_snapshots_parser.add_argument("--limit", type=int, default=0)
    entity_snapshots_parser.add_argument("--format", choices=["json", "ndjson"], default="json")
    entity_snapshots_parser.set_defaults(handler=handle_entity_snapshots)

    entity_attr_parser = entity_commands.add_parser("attr", help="Get an entity attribute value.")
    entity_attr_parser.add_argument("etype")
    entity_attr_parser.add_argument("eid")
    entity_attr_parser.add_argument("attr")
    entity_attr_parser.add_argument("--from", dest="date_from")
    entity_attr_parser.add_argument("--to", dest="date_to")
    entity_attr_parser.set_defaults(handler=handle_entity_attr)

    entity_distinct_parser = entity_commands.add_parser(
        "distinct", help="Get distinct latest values of an attribute."
    )
    entity_distinct_parser.add_argument("etype")
    entity_distinct_parser.add_argument("attr")
    entity_distinct_parser.set_defaults(handler=handle_entity_distinct)

    entity_set_parser = entity_commands.add_parser(
        "set", help="Set a current entity attribute value."
    )
    entity_set_parser.add_argument("etype")
    entity_set_parser.add_argument("eid")
    entity_set_parser.add_argument("attr")
    entity_set_parser.add_argument("--value-json", required=True)
    entity_set_parser.set_defaults(handler=handle_entity_set)

    entity_ttl_parser = entity_commands.add_parser("ttl", help="Extend entity TTLs.")
    entity_ttl_parser.add_argument("etype")
    entity_ttl_parser.add_argument("eid")
    entity_ttl_parser.add_argument("--body-json", required=True)
    entity_ttl_parser.set_defaults(handler=handle_entity_ttl)

    entity_delete_parser = entity_commands.add_parser("delete", help="Delete entity data.")
    entity_delete_parser.add_argument("etype")
    entity_delete_parser.add_argument("eid")
    entity_delete_parser.set_defaults(handler=handle_entity_delete)

    control_parser = commands.add_parser("control", help="Execute control actions.")
    control_commands = control_parser.add_subparsers(dest="control_command", required=True)

    make_snapshots_parser = control_commands.add_parser(
        "make-snapshots", help="Trigger an out-of-order snapshot run."
    )
    make_snapshots_parser.set_defaults(handler=handle_control_make_snapshots)

    refresh_entity_creation_parser = control_commands.add_parser(
        "refresh-on-entity-creation",
        help="Re-run entity creation callbacks for an entity type.",
    )
    refresh_entity_creation_parser.add_argument("etype")
    refresh_entity_creation_parser.set_defaults(handler=handle_control_refresh_on_entity_creation)

    refresh_module_config_parser = control_commands.add_parser(
        "refresh-module-config",
        help="Reload module configuration.",
    )
    refresh_module_config_parser.add_argument("module")
    refresh_module_config_parser.set_defaults(handler=handle_control_refresh_module_config)

    telemetry_parser = commands.add_parser("telemetry", help="Read operational telemetry.")
    telemetry_commands = telemetry_parser.add_subparsers(dest="telemetry_command", required=True)

    sources_validity_parser = telemetry_commands.add_parser(
        "sources-validity", help="Show source validity timestamps."
    )
    sources_validity_parser.set_defaults(handler=handle_telemetry_sources_validity)

    source_age_parser = telemetry_commands.add_parser("source-age", help="Show source ages.")
    source_age_parser.add_argument("--unit", choices=["minutes", "seconds"], default="minutes")
    source_age_parser.set_defaults(handler=handle_telemetry_source_age)

    entities_per_attr_parser = telemetry_commands.add_parser(
        "entities-per-attr", help="Count entities with data present for each attribute."
    )
    entities_per_attr_parser.set_defaults(handler=handle_telemetry_entities_per_attr)

    snapshot_summary_parser = telemetry_commands.add_parser(
        "snapshot-summary", help="Show recent snapshot activity summary."
    )
    snapshot_summary_parser.set_defaults(handler=handle_telemetry_snapshot_summary)

    metadata_parser = telemetry_commands.add_parser(
        "metadata", help="Browse internal metadata records."
    )
    metadata_parser.add_argument("--module")
    metadata_parser.add_argument("--from", dest="date_from")
    metadata_parser.add_argument("--to", dest="date_to")
    metadata_parser.add_argument("--skip", type=int, default=0)
    metadata_parser.add_argument("--limit", type=int, default=0)
    metadata_parser.add_argument("--sort", choices=["newest", "oldest"], default="newest")
    metadata_parser.add_argument("--format", choices=["json", "ndjson"], default="json")
    metadata_parser.set_defaults(handler=handle_telemetry_metadata)

    rabbitmq_queues_parser = telemetry_commands.add_parser(
        "rabbitmq-queues", help="Show RabbitMQ queue telemetry."
    )
    rabbitmq_queues_parser.set_defaults(handler=handle_telemetry_rabbitmq_queues)


def run() -> None:
    parser = argparse.ArgumentParser(prog="dp3 sh")
    init_parser(parser)
    args = parser.parse_args()
    sys.exit(main(args))


def main(args) -> int:
    try:
        config = read_config_dir(os.path.abspath(args.config), recursive=True)
        model_spec = ModelSpec(config.get("db_entities"))
    except Exception as e:
        print(f"Cannot read config directory '{args.config}': {e}", file=sys.stderr)
        return 1

    try:
        client = DP3APIClient(args.config, args.url, args.timeout, model_spec=model_spec)
        return args.handler(client, args)
    except APIError as e:
        print(str(e), file=sys.stderr)
        return 1


if __name__ == "__main__":
    run()
