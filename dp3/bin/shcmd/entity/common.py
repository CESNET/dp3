#!/usr/bin/env python3
"""Shared helpers for entity commands in the shell-oriented DP3 CLI."""

import argparse
import json
from collections.abc import Iterable, Sequence
from typing import Any, Optional

from dp3.bin.shcmd.common import (
    JSON_LITERAL_HELP,
    APIError,
    common_time_params,
    get_completion_context,
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
ENTITY_TYPE_COMMANDS = ["list", "count", "raw", "attr"]
ENTITY_INSTANCE_COMMANDS = ["get", "master", "snapshots", "raw", "ttl", "delete", "attr"]
ENTITY_ID_PLACEHOLDER = "<EID>"
TYPE_LIST_OPTIONS = ["--fulltext-json", "--filter-json", "--has-attr", "--skip", "--limit"]
TYPE_COUNT_OPTIONS = ["--fulltext-json", "--filter-json", "--has-attr"]
RAW_OPTIONS = ["--attr", "--src", "--skip", "--limit", "--format"]
TIME_RANGE_OPTIONS = ["--from", "--to"]
SNAPSHOT_OPTIONS = ["--from", "--to", "--skip", "--limit", "--format"]


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


def _filter_matches(values: Iterable[str], prefix: str) -> list[str]:
    return [value for value in values if value.startswith(prefix)]


def _entity_types(model_spec, entity_catalog: Optional[dict[str, Any]] = None) -> list[str]:
    if model_spec is not None:
        return sorted(model_spec.entities)
    if entity_catalog is not None:
        return sorted(entity_catalog)
    return []


def _entity_attrs(
    model_spec, etype: str, entity_catalog: Optional[dict[str, Any]] = None
) -> list[str]:
    if model_spec is not None and etype in model_spec.entities:
        return sorted(model_spec.attribs(etype))
    if entity_catalog is not None and etype in entity_catalog:
        return sorted(entity_catalog[etype].get("attribs", []))
    return []


def _complete_options(
    args: Sequence[str],
    prefix: str,
    *,
    options: Sequence[str],
    value_choices: Optional[dict[str, Sequence[str]]] = None,
) -> list[str]:
    value_choices = value_choices or {}
    if args and args[-1] in value_choices:
        return _filter_matches(value_choices[args[-1]], prefix)
    return _filter_matches(options, prefix)


def complete_entity_selector(prefix: str, parsed_args, **_kwargs) -> list[str]:
    """Complete entity types for the path-like entity command."""
    model_spec, entity_catalog = get_completion_context(parsed_args)
    return _filter_matches(_entity_types(model_spec, entity_catalog), prefix)


def complete_entity_rest(prefix: str, parsed_args, **_kwargs) -> list[str]:  # noqa: PLR0911
    """Complete type-scope and single-entity commands under `entity`."""
    model_spec, entity_catalog = get_completion_context(parsed_args)
    etype = getattr(parsed_args, "selector", None)
    if etype is None:
        return []

    words = list(getattr(parsed_args, "rest", []))
    attrs = _entity_attrs(model_spec, etype, entity_catalog)
    if not words:
        matches = _filter_matches(ENTITY_TYPE_COMMANDS, prefix)
        if not prefix or not prefix.startswith("-"):
            matches.append(ENTITY_ID_PLACEHOLDER)
        return matches

    command = words[0]
    args = words[1:]
    if command == "list":
        return _complete_options(
            args,
            prefix,
            options=TYPE_LIST_OPTIONS,
            value_choices={"--has-attr": attrs},
        )
    if command == "count":
        return _complete_options(
            args,
            prefix,
            options=TYPE_COUNT_OPTIONS,
            value_choices={"--has-attr": attrs},
        )
    if command == "raw":
        return _complete_options(
            args,
            prefix,
            options=RAW_OPTIONS,
            value_choices={"--attr": attrs, "--format": ["json", "ndjson"]},
        )
    if command == "attr":
        if not args:
            return _filter_matches(attrs, prefix)
        if len(args) == 1:
            return _filter_matches(["distinct"], prefix)
        return []
    if command in ENTITY_TYPE_COMMANDS:
        return _filter_matches(ENTITY_TYPE_COMMANDS, prefix)

    if not args:
        return _filter_matches(ENTITY_INSTANCE_COMMANDS, prefix)

    entity_command = args[0]
    entity_args = args[1:]
    if entity_command in {"get", "master"}:
        return _complete_options(entity_args, prefix, options=TIME_RANGE_OPTIONS)
    if entity_command == "snapshots":
        return _complete_options(
            entity_args,
            prefix,
            options=SNAPSHOT_OPTIONS,
            value_choices={"--format": ["json", "ndjson"]},
        )
    if entity_command == "raw":
        return _complete_options(
            entity_args,
            prefix,
            options=RAW_OPTIONS,
            value_choices={"--attr": attrs, "--format": ["json", "ndjson"]},
        )
    if entity_command == "ttl":
        return _filter_matches(["--body-json"], prefix)
    if entity_command == "delete":
        return []
    if entity_command == "attr":
        if not entity_args:
            return _filter_matches(attrs, prefix)
        attr = entity_args[0]
        attr_args = entity_args[1:]
        if not attr_args:
            return _filter_matches(["get", "set"], prefix)
        if attr_args[0] == "get":
            return _complete_options(attr_args[1:], prefix, options=TIME_RANGE_OPTIONS)
        if attr_args[0] == "set":
            return []
        if attr.startswith(prefix):
            return [attr]
        return []
    return _filter_matches(ENTITY_INSTANCE_COMMANDS, prefix)
