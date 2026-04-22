#!/usr/bin/env python3
"""Entity type-scope commands for the shell-oriented DP3 CLI."""

import argparse

from dp3.bin.shcmd.common import print_response_json

from .common import (
    RAW_HELP,
    add_ndjson_format_arg,
    add_page_args,
    add_raw_filter_args,
    add_type_filter_args,
    build_type_query_params,
    handle_raw,
)

TYPE_COMMANDS = {"list", "count", "raw", "attr", "-h", "--help"}


def handle_list(client, args) -> int:
    """List latest entity snapshots for one type."""
    params = build_type_query_params(client, args, include_paging=True)
    return print_response_json(client.request("GET", f"/entity/{args.etype}/get", params=params))


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
    parser = argparse.ArgumentParser(
        prog=f"dp3 sh entity {etype}",
        description=(
            f"Query entities of type '{etype}' or continue with an entity id to inspect one entity."
        ),
    )
    commands = parser.add_subparsers(dest="entity_type_command", required=True)

    list_parser = commands.add_parser("list", help="List latest entity snapshots.")
    add_type_filter_args(list_parser, include_paging=True)
    list_parser.set_defaults(handler=handle_list, etype=etype)

    count_parser = commands.add_parser("count", help="Count latest entity snapshots.")
    add_type_filter_args(count_parser, include_paging=False)
    count_parser.set_defaults(handler=handle_count, etype=etype)

    raw_parser = commands.add_parser("raw", help=RAW_HELP)
    add_raw_filter_args(raw_parser)
    add_page_args(raw_parser, default_limit=20)
    add_ndjson_format_arg(raw_parser)
    raw_parser.set_defaults(handler=handle_raw, etype=etype, eid=None)

    attr_parser = commands.add_parser(
        "attr", help="Query a single attribute across the entity type."
    )
    attr_parser.add_argument("attr", metavar="ATTR")
    attr_commands = attr_parser.add_subparsers(dest="entity_type_attr_command", required=True)
    distinct_parser = attr_commands.add_parser(
        "distinct", help="Get distinct latest values of an attribute."
    )
    distinct_parser.set_defaults(handler=handle_distinct, etype=etype)

    return parser
