#!/usr/bin/env python3
"""Shared helpers for entity commands in the shell-oriented DP3 CLI."""

import argparse
import json
from typing import Any, Optional

from dp3.bin.shcmd.common import (
    JSON_LITERAL_HELP,
    APIError,
    common_time_params,
    print_response_json,
    read_json_object,
    read_json_value,
    stream_json_pages,
)
from dp3.common.attrspec import AttrType

RAW_HELP = (
    "Browse current raw datapoints for troubleshooting ingestion. "
    "Can be slow on large raw collections."
)
ATTR_VALUE_HELP = "JSON literal value, for example '\"hello\"', '42', 'true', or '{\"k\":1}'."


def add_time_range_args(parser: argparse.ArgumentParser) -> None:
    """Add common time-range arguments to a parser."""
    parser.add_argument("--from", dest="date_from")
    parser.add_argument("--to", dest="date_to")


def add_page_args(parser: argparse.ArgumentParser, *, default_limit: int) -> None:
    """Add common paging arguments to a parser."""
    parser.add_argument("--skip", type=int, default=0)
    parser.add_argument("--limit", type=int, default=default_limit)


def add_ndjson_format_arg(parser: argparse.ArgumentParser) -> None:
    """Add common JSON/NDJSON output selection."""
    parser.add_argument("--format", choices=["json", "ndjson"], default="json")


def add_raw_filter_args(parser: argparse.ArgumentParser) -> None:
    """Add common raw datapoint filters to a parser."""
    parser.add_argument("--attr")
    parser.add_argument("--src")


def add_type_filter_args(
    parser: argparse.ArgumentParser,
    *,
    include_paging: bool,
    default_limit: int = 20,
) -> None:
    """Add common entity-type filters to a parser."""
    parser.add_argument("--fulltext-json", default=None)
    parser.add_argument("--filter-json", default=None)
    parser.add_argument(
        "--has-attr",
        default=None,
        help="Limit results to latest snapshots where the attribute has data present.",
    )
    if include_paging:
        add_page_args(parser, default_limit=default_limit)


def build_has_attr_filter(client, etype: str, attr: str) -> dict[str, Any]:
    """Build a generic-filter clause for entities with data present for one attribute."""
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


def build_generic_filter_param(client, args) -> Optional[str]:
    """Build the generic-filter query parameter for entity type queries."""
    query = None
    if getattr(args, "filter_json", None) is not None:
        query = read_json_object(args.filter_json, "--filter-json")
    if getattr(args, "has_attr", None) is not None:
        has_attr_filter = build_has_attr_filter(client, args.etype, args.has_attr)
        query = has_attr_filter if query is None else {"$and": [query, has_attr_filter]}
    if query is None:
        return None
    return json.dumps(query)


def build_type_query_params(client, args, *, include_paging: bool) -> dict[str, Any]:
    """Build query parameters shared by list and count operations."""
    params = {}
    if include_paging:
        params.update({"skip": args.skip, "limit": args.limit})
    if args.fulltext_json is not None:
        params["fulltext_filters"] = args.fulltext_json
    generic_filter = build_generic_filter_param(client, args)
    if generic_filter is not None:
        params["generic_filter"] = generic_filter
    return params


def build_raw_params(args) -> dict[str, Any]:
    """Build raw-datapoint query parameters."""
    params = {"skip": args.skip, "limit": args.limit}
    if getattr(args, "eid", None) is not None:
        params["eid"] = args.eid
    if args.attr is not None:
        params["attr"] = args.attr
    if args.src is not None:
        params["src"] = args.src
    return params


def handle_raw(client, args) -> int:
    """Browse current raw datapoints for one type or one entity."""
    params = build_raw_params(args)
    path = f"/entity/{args.etype}/raw/get"
    if args.format == "ndjson":
        base_params = {key: value for key, value in params.items() if key not in {"skip", "limit"}}
        return stream_json_pages(client, path, base_params, args.skip, args.limit)
    return print_response_json(client.request("GET", path, params=params))


def handle_attr_set_request(client, args) -> int:
    """Set one current entity attribute value."""
    value_json = args.value_json
    if value_json is None:
        value_json = getattr(args, "value_json_flag", None)
    if value_json is None:
        raise APIError(JSON_LITERAL_HELP)
    body = {"value": read_json_value(value_json)}
    return print_response_json(
        client.request("POST", f"/entity/{args.etype}/{args.eid}/set/{args.attr}", json_body=body)
    )


def add_entity_attr_set_args(parser: argparse.ArgumentParser) -> None:
    """Add the value argument for entity attribute updates."""
    parser.add_argument("value_json", metavar="VALUE_JSON", help=ATTR_VALUE_HELP)
    parser.add_argument("--value-json", dest="value_json_flag", help=argparse.SUPPRESS)


def build_time_page_params(args) -> dict[str, Any]:
    """Build query parameters for time-ranged, paged entity resources."""
    params = common_time_params(args)
    params["skip"] = args.skip
    params["limit"] = args.limit
    return params
