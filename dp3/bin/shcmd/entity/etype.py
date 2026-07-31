#!/usr/bin/env python3
"""Entity type-scope commands for the shell-oriented DP3 CLI."""

import argparse

from dp3.bin.shcmd.common import (
    MarkdownHelpArgumentParser,
    command_description,
    print_response_json,
    stream_json_pages,
)

from . import instance
from .common import (
    RAW_HELP,
    add_ndjson_format_arg,
    add_page_args,
    add_raw_filter_args,
    add_type_filter_args,
    build_type_query_params,
    complete_entity_attr_names,
    handle_raw,
)


def handle_list(client, args) -> int:
    """List latest entity snapshots for one type."""
    path = f"/entity/{args.etype}/get"
    params = build_type_query_params(client, args, include_paging=True)
    if args.format == "ndjson":
        base_params = {key: value for key, value in params.items() if key not in {"skip", "limit"}}
        return stream_json_pages(client, path, base_params, args.skip, args.limit)
    return print_response_json(client.request("GET", path, params=params))


def handle_count(client, args) -> int:
    """Count latest entity snapshots for one type."""
    params = build_type_query_params(client, args, include_paging=False)
    return print_response_json(client.request("GET", f"/entity/{args.etype}/count", params=params))


def handle_distinct(client, args) -> int:
    """Get distinct values for one attribute across an entity type."""
    return print_response_json(
        client.request("GET", f"/entity/{args.etype}/_/distinct/{args.attr}")
    )


def build_parser(etype: str) -> argparse.ArgumentParser:
    """Build the parser for entity type-scope commands."""
    parser = MarkdownHelpArgumentParser(
        prog=f"dp3 sh entity {etype}",
        description=f"Query entities of type `{etype}` or select one entity by id.",
    )
    parser.set_defaults(etype=etype)
    commands = parser.add_subparsers(dest="entity_type_command", required=True)

    list_parser = commands.add_parser(
        "list",
        help="List latest entity snapshots.",
        description=f"List latest snapshots for entities of type `{etype}`.",
    )
    add_type_filter_args(list_parser, include_paging=True)
    add_ndjson_format_arg(list_parser)
    list_parser.set_defaults(handler=handle_list, etype=etype)

    count_parser = commands.add_parser(
        "count",
        help="Count latest entity snapshots.",
        description=command_description(
            f"Count latest snapshots for entities of type `{etype}`. JSON filters use "
            "MongoDB query syntax.",
            f"dp3 sh entity {etype} count --filter-json '{{\"last.active\":true}}'",
        ),
    )
    add_type_filter_args(count_parser, include_paging=False)
    count_parser.set_defaults(handler=handle_count, etype=etype)

    raw_parser = commands.add_parser(
        "raw",
        help=RAW_HELP,
        description=command_description(
            f"Browse current raw datapoints for entities of type `{etype}`.",
            f"dp3 sh entity {etype} raw --attr ATTR --src SOURCE --limit 100 --format ndjson",
        ),
    )
    add_raw_filter_args(raw_parser)
    add_page_args(raw_parser, default_limit=20, subject="raw datapoints")
    add_ndjson_format_arg(raw_parser)
    raw_parser.set_defaults(handler=handle_raw, etype=etype, eid=None)

    attr_values_parser = commands.add_parser(
        "attr-values",
        help="Get distinct latest values of one attribute across the entity type.",
        description=(
            f"Get distinct latest values of an attribute across entities of type `{etype}`."
        ),
    )
    attr_action = attr_values_parser.add_argument(
        "attr", metavar="ATTR", help="Attribute to query across the entity type."
    )
    attr_action.completer = complete_entity_attr_names
    attr_values_parser.set_defaults(handler=handle_distinct, etype=etype)

    instance.add_id_parser(commands, etype)

    return parser
