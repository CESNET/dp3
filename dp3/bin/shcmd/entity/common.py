#!/usr/bin/env python3
"""Shared helpers for entity commands in the shell-oriented DP3 CLI."""

import argparse
import json
from typing import Any, Optional

from argcomplete.finders import CompletionFinder

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
ENTITY_ID_PLACEHOLDER = "<EID>"


def suppress_completion(_prefix: str, **_kwargs) -> dict[str, str]:
    """Return no completion candidates for free-form values."""
    return {}


def add_time_range_args(
    parser: argparse.ArgumentParser,
    *,
    scope: str = "time range",
) -> None:
    """Add common time-range arguments to a parser."""
    from_action = parser.add_argument(
        "-f",
        "--from",
        dest="date_from",
        help=f"Lower bound of the {scope}.",
    )
    from_action.completer = suppress_completion
    to_action = parser.add_argument(
        "-t",
        "--to",
        dest="date_to",
        help=f"Upper bound of the {scope}.",
    )
    to_action.completer = suppress_completion


def add_page_args(
    parser: argparse.ArgumentParser,
    *,
    default_limit: int,
    subject: str = "results",
) -> None:
    """Add common paging arguments to a parser."""
    skip_action = parser.add_argument(
        "-s",
        "--skip",
        type=int,
        default=0,
        help=f"Skip this many {subject} before returning data.",
    )
    skip_action.completer = suppress_completion
    limit_action = parser.add_argument(
        "-l",
        "--limit",
        type=int,
        default=default_limit,
        help=f"Return at most this many {subject}.",
    )
    limit_action.completer = suppress_completion


def add_ndjson_format_arg(parser: argparse.ArgumentParser) -> None:
    """Add common JSON/NDJSON output selection."""
    parser.add_argument(
        "-F",
        "--format",
        choices=["json", "ndjson"],
        default="json",
        help="Choose JSON or NDJSON output.",
    )


def add_raw_filter_args(parser: argparse.ArgumentParser) -> None:
    """Add common raw datapoint filters to a parser."""
    attr_action = parser.add_argument(
        "-a",
        "--attr",
        help="Limit raw datapoints to one attribute.",
    )
    attr_action.completer = complete_entity_attr_names
    src_action = parser.add_argument(
        "-r",
        "--src",
        help="Limit raw datapoints to one source.",
    )
    src_action.completer = suppress_completion


def add_type_filter_args(
    parser: argparse.ArgumentParser,
    *,
    include_paging: bool,
    default_limit: int = 20,
) -> None:
    """Add common entity-type filters to a parser."""
    fulltext_action = parser.add_argument(
        "-q",
        "--fulltext-json",
        default=None,
        help="JSON object with fulltext search filters.",
    )
    fulltext_action.completer = suppress_completion
    filter_action = parser.add_argument(
        "-j",
        "--filter-json",
        default=None,
        help="JSON object with additional generic filters.",
    )
    filter_action.completer = suppress_completion
    has_attr_action = parser.add_argument(
        "-a",
        "--has-attr",
        default=None,
        help="Limit results to latest snapshots where the attribute has data present.",
    )
    has_attr_action.completer = complete_entity_attr_names
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
    value_action = parser.add_argument("value_json", metavar="VALUE_JSON", help=ATTR_VALUE_HELP)
    value_action.completer = suppress_completion
    value_flag_action = parser.add_argument(
        "-v",
        "--value-json",
        dest="value_json_flag",
        help=argparse.SUPPRESS,
    )
    value_flag_action.completer = suppress_completion


def build_time_page_params(args) -> dict[str, Any]:
    """Build query parameters for time-ranged, paged entity resources."""
    params = common_time_params(args)
    params["skip"] = args.skip
    params["limit"] = args.limit
    return params


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


def _normalize_completion(value: str) -> str:
    return value.rstrip()


def _complete_from_parser(
    parser: argparse.ArgumentParser,
    words: list[str],
    prefix: str,
    parsed_args,
) -> dict[str, str]:
    parser.set_defaults(
        config=getattr(parsed_args, "config", None),
        url=getattr(parsed_args, "url", None),
        timeout=getattr(parsed_args, "timeout", 5.0),
    )
    finder = CompletionFinder(parser, always_complete_options=False)
    values = finder._get_completions([parser.prog, *words], prefix, "", None)
    descriptions = getattr(finder, "_display_completions", {})
    return {
        _normalize_completion(value): descriptions.get(_normalize_completion(value), "")
        for value in values
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


def complete_entity_attr_names(prefix: str, parsed_args, **_kwargs) -> dict[str, str]:
    """Complete attribute names for one entity type."""
    etype = getattr(parsed_args, "etype", None) or getattr(parsed_args, "selector", None)
    if etype is None:
        return {}
    model_spec, entity_catalog = get_completion_context(parsed_args)
    return _match_descriptions(_entity_attr_descriptions(model_spec, etype, entity_catalog), prefix)


def complete_entity_rest(prefix: str, parsed_args, **_kwargs) -> dict[str, str]:
    """Complete type-scope and single-entity commands under `entity`."""
    etype = getattr(parsed_args, "selector", None)
    if etype is None:
        return {}

    from . import etype as entity_etype

    words = list(getattr(parsed_args, "rest", []))
    return _complete_from_parser(entity_etype.build_parser(etype), words, prefix, parsed_args)
