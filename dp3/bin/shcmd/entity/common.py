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
ENTITY_TYPE_COMMANDS = ["list", "count", "raw", "attr-values"]
ENTITY_INSTANCE_COMMANDS = ["get", "master", "snapshots", "raw", "ttl", "delete", "attr"]
ENTITY_ID_PLACEHOLDER = "<EID>"
TYPE_LIST_OPTIONS = ["--fulltext-json", "--filter-json", "--has-attr", "--skip", "--limit"]
TYPE_COUNT_OPTIONS = ["--fulltext-json", "--filter-json", "--has-attr"]
RAW_OPTIONS = ["--attr", "--src", "--skip", "--limit", "--format"]
TIME_RANGE_OPTIONS = ["--from", "--to"]
SNAPSHOT_OPTIONS = ["--from", "--to", "--skip", "--limit", "--format"]
TYPE_COMMAND_DESCRIPTIONS = {
    "list": "List latest entity snapshots for the entity type.",
    "count": "Count latest entity snapshots for the entity type.",
    "raw": RAW_HELP,
    "attr-values": "Get distinct latest values of one attribute across the entity type.",
}
INSTANCE_COMMAND_DESCRIPTIONS = {
    "get": "Get the combined entity view.",
    "master": "Get the current master record.",
    "snapshots": "Browse snapshots of a single entity.",
    "raw": RAW_HELP,
    "ttl": "Extend TTL values for the entity.",
    "delete": "Delete the entity data.",
    "attr": "Get or modify one entity attribute.",
}
TYPE_OPTION_DESCRIPTIONS = {
    "--fulltext-json": "JSON object with fulltext search filters.",
    "--filter-json": "JSON object with additional generic filters.",
    "--has-attr": "Limit results to entities where the attribute has data present.",
    "--skip": "Skip this many results before returning data.",
    "--limit": "Return at most this many results.",
}
RAW_OPTION_DESCRIPTIONS = {
    "--attr": "Limit raw datapoints to one attribute.",
    "--src": "Limit raw datapoints to one source.",
    "--skip": "Skip this many raw datapoints before returning data.",
    "--limit": "Return at most this many raw datapoints.",
    "--format": "Choose JSON or NDJSON output.",
}
TIME_RANGE_OPTION_DESCRIPTIONS = {
    "--from": "Lower bound of the time range.",
    "--to": "Upper bound of the time range.",
}
SNAPSHOT_OPTION_DESCRIPTIONS = {
    "--from": "Lower bound of the snapshot time range.",
    "--to": "Upper bound of the snapshot time range.",
    "--skip": "Skip this many snapshots before returning data.",
    "--limit": "Return at most this many snapshots.",
    "--format": "Choose JSON or NDJSON output.",
}
VALUE_CHOICE_DESCRIPTIONS = {
    "json": "Return structured JSON output.",
    "ndjson": "Return newline-delimited JSON output.",
}


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


def _match_descriptions(values: dict[str, str], prefix: str) -> dict[str, str]:
    return {value: description for value, description in values.items() if value.startswith(prefix)}


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


def _entity_attr_descriptions(
    model_spec, etype: str, entity_catalog: Optional[dict[str, Any]] = None
) -> dict[str, str]:
    attrs = _entity_attrs(model_spec, etype, entity_catalog)
    descriptions = {attr: f"Attribute on entity type '{etype}'." for attr in attrs}
    if model_spec is not None and etype in model_spec.entities:
        for attr in attrs:
            attr_spec = model_spec.attr(etype, attr)
            descriptions[attr] = f"{attr_spec.t.name.lower()} attribute on entity type '{etype}'."
    return descriptions


def _complete_options(
    args: Sequence[str],
    prefix: str,
    *,
    options: Sequence[str],
    option_descriptions: Optional[dict[str, str]] = None,
    value_choices: Optional[dict[str, Sequence[str]]] = None,
    value_descriptions: Optional[dict[str, dict[str, str]]] = None,
) -> dict[str, str]:
    option_descriptions = option_descriptions or {}
    value_choices = value_choices or {}
    value_descriptions = value_descriptions or {}
    if args and args[-1] in value_choices:
        choices = value_choices[args[-1]]
        descriptions = value_descriptions.get(args[-1], {})
        return {
            choice: descriptions.get(choice, f"Value for {args[-1]}.")
            for choice in choices
            if choice.startswith(prefix)
        }
    return {
        option: option_descriptions.get(option, "Command option.")
        for option in options
        if option.startswith(prefix)
    }


def complete_entity_selector(prefix: str, parsed_args, **_kwargs) -> dict[str, str]:
    """Complete entity types for the path-like entity command."""
    model_spec, entity_catalog = get_completion_context(parsed_args)
    attr_descriptions = {}
    for etype in _entity_types(model_spec, entity_catalog):
        if model_spec is not None and etype in model_spec.entities:
            entity_spec = model_spec.entity(etype)
            attr_descriptions[etype] = f"{entity_spec.name} ({entity_spec.id_data_type.root} ids)"
        elif entity_catalog is not None and etype in entity_catalog:
            entry = entity_catalog[etype]
            name = entry.get("name") or etype
            id_data_type = entry.get("id_data_type")
            if id_data_type:
                attr_descriptions[etype] = f"{name} ({id_data_type} ids)"
            else:
                attr_descriptions[etype] = str(name)
        else:
            attr_descriptions[etype] = "Configured entity type."
    return _match_descriptions(attr_descriptions, prefix)


def complete_entity_rest(prefix: str, parsed_args, **_kwargs) -> dict[str, str]:  # noqa: PLR0911
    """Complete type-scope and single-entity commands under `entity`."""
    model_spec, entity_catalog = get_completion_context(parsed_args)
    etype = getattr(parsed_args, "selector", None)
    if etype is None:
        return {}

    words = list(getattr(parsed_args, "rest", []))
    attr_descriptions = _entity_attr_descriptions(model_spec, etype, entity_catalog)
    attrs = list(attr_descriptions)
    if not words:
        matches = _match_descriptions(TYPE_COMMAND_DESCRIPTIONS, prefix)
        if not prefix or not prefix.startswith("-"):
            matches[ENTITY_ID_PLACEHOLDER] = "Enter an entity id to inspect one entity."
        return matches

    command = words[0]
    args = words[1:]
    if command == "list":
        return _complete_options(
            args,
            prefix,
            options=TYPE_LIST_OPTIONS,
            option_descriptions=TYPE_OPTION_DESCRIPTIONS,
            value_choices={"--has-attr": attrs},
            value_descriptions={"--has-attr": attr_descriptions},
        )
    if command == "count":
        return _complete_options(
            args,
            prefix,
            options=TYPE_COUNT_OPTIONS,
            option_descriptions=TYPE_OPTION_DESCRIPTIONS,
            value_choices={"--has-attr": attrs},
            value_descriptions={"--has-attr": attr_descriptions},
        )
    if command == "raw":
        return _complete_options(
            args,
            prefix,
            options=RAW_OPTIONS,
            option_descriptions=RAW_OPTION_DESCRIPTIONS,
            value_choices={"--attr": attrs, "--format": ["json", "ndjson"]},
            value_descriptions={
                "--attr": attr_descriptions,
                "--format": VALUE_CHOICE_DESCRIPTIONS,
            },
        )
    if command == "attr-values":
        if not args:
            return _match_descriptions(attr_descriptions, prefix)
        return {}
    if command in ENTITY_TYPE_COMMANDS:
        return _match_descriptions(TYPE_COMMAND_DESCRIPTIONS, prefix)

    if not args:
        return _match_descriptions(INSTANCE_COMMAND_DESCRIPTIONS, prefix)

    entity_command = args[0]
    entity_args = args[1:]
    if entity_command in {"get", "master"}:
        return _complete_options(
            entity_args,
            prefix,
            options=TIME_RANGE_OPTIONS,
            option_descriptions=TIME_RANGE_OPTION_DESCRIPTIONS,
        )
    if entity_command == "snapshots":
        return _complete_options(
            entity_args,
            prefix,
            options=SNAPSHOT_OPTIONS,
            option_descriptions=SNAPSHOT_OPTION_DESCRIPTIONS,
            value_choices={"--format": ["json", "ndjson"]},
            value_descriptions={"--format": VALUE_CHOICE_DESCRIPTIONS},
        )
    if entity_command == "raw":
        return _complete_options(
            entity_args,
            prefix,
            options=RAW_OPTIONS,
            option_descriptions=RAW_OPTION_DESCRIPTIONS,
            value_choices={"--attr": attrs, "--format": ["json", "ndjson"]},
            value_descriptions={
                "--attr": attr_descriptions,
                "--format": VALUE_CHOICE_DESCRIPTIONS,
            },
        )
    if entity_command == "ttl":
        return _match_descriptions(
            {"--body-json": "JSON body describing the TTL update request."}, prefix
        )
    if entity_command == "delete":
        return {}
    if entity_command == "attr":
        if not entity_args:
            return _match_descriptions(attr_descriptions, prefix)
        attr = entity_args[0]
        attr_args = entity_args[1:]
        if not attr_args:
            return _match_descriptions(
                {
                    "get": "Get the current value of this attribute.",
                    "set": "Set the current value of this attribute.",
                },
                prefix,
            )
        if attr_args[0] == "get":
            return _complete_options(
                attr_args[1:],
                prefix,
                options=TIME_RANGE_OPTIONS,
                option_descriptions=TIME_RANGE_OPTION_DESCRIPTIONS,
            )
        if attr_args[0] == "set":
            return {}
        if attr.startswith(prefix):
            return {attr: attr_descriptions.get(attr, f"Attribute on entity type '{etype}'.")}
        return {}
    return _match_descriptions(INSTANCE_COMMAND_DESCRIPTIONS, prefix)
