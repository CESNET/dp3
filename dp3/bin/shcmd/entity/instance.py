#!/usr/bin/env python3
"""Single-entity commands for the shell-oriented DP3 CLI."""

import argparse

from dp3.bin.shcmd.common import (
    common_time_params,
    print_response_json,
    read_json_value,
    stream_json_pages,
)

from . import attr
from .common import (
    RAW_HELP,
    add_ndjson_format_arg,
    add_page_args,
    add_raw_filter_args,
    add_time_range_args,
    build_time_page_params,
    handle_raw,
    suppress_completion,
)


def handle_get(client, args) -> int:
    """Get full entity data."""
    return print_response_json(
        client.request("GET", f"/entity/{args.etype}/{args.eid}", params=common_time_params(args))
    )


def handle_master(client, args) -> int:
    """Get the master record for one entity."""
    return print_response_json(
        client.request(
            "GET", f"/entity/{args.etype}/{args.eid}/master", params=common_time_params(args)
        )
    )


def handle_snapshots(client, args) -> int:
    """Get snapshots for one entity."""
    path = f"/entity/{args.etype}/{args.eid}/snapshots"
    if args.format == "ndjson":
        return stream_json_pages(client, path, common_time_params(args), args.skip, args.limit)
    return print_response_json(client.request("GET", path, params=build_time_page_params(args)))


def handle_ttl(client, args) -> int:
    """Extend entity TTLs."""
    body = read_json_value(args.body_json)
    return print_response_json(
        client.request("POST", f"/entity/{args.etype}/{args.eid}/ttl", json_body=body)
    )


def handle_delete(client, args) -> int:
    """Delete entity data."""
    return print_response_json(client.request("DELETE", f"/entity/{args.etype}/{args.eid}"))


def build_parser(etype: str, eid: str) -> argparse.ArgumentParser:
    """Build the parser for single-entity commands."""
    parser = argparse.ArgumentParser(
        prog=f"dp3 sh entity {etype} {eid}",
        description=f"Inspect or modify entity '{etype}/{eid}'.",
    )
    parser.set_defaults(etype=etype, eid=eid)
    commands = parser.add_subparsers(dest="entity_instance_command", required=True)

    get_parser = commands.add_parser("get", help="Get full entity data.")
    add_time_range_args(get_parser)
    get_parser.set_defaults(handler=handle_get, etype=etype, eid=eid)

    master_parser = commands.add_parser("master", help="Get an entity master record.")
    add_time_range_args(master_parser)
    master_parser.set_defaults(handler=handle_master, etype=etype, eid=eid)

    snapshots_parser = commands.add_parser("snapshots", help="Get snapshots of a single entity.")
    add_time_range_args(snapshots_parser, scope="snapshot time range")
    add_page_args(snapshots_parser, default_limit=0, subject="snapshots")
    add_ndjson_format_arg(snapshots_parser)
    snapshots_parser.set_defaults(handler=handle_snapshots, etype=etype, eid=eid)

    raw_parser = commands.add_parser("raw", help=RAW_HELP)
    add_raw_filter_args(raw_parser)
    add_page_args(raw_parser, default_limit=20, subject="raw datapoints")
    add_ndjson_format_arg(raw_parser)
    raw_parser.set_defaults(handler=handle_raw, etype=etype, eid=eid)

    attr.add_instance_attr_parser(commands, etype, eid)

    ttl_parser = commands.add_parser("ttl", help="Extend entity TTLs.")
    body_action = ttl_parser.add_argument(
        "--body-json",
        required=True,
        help="JSON body describing the TTL update request.",
    )
    body_action.completer = suppress_completion
    ttl_parser.set_defaults(handler=handle_ttl, etype=etype, eid=eid)

    delete_parser = commands.add_parser("delete", help="Delete entity data.")
    delete_parser.set_defaults(handler=handle_delete, etype=etype, eid=eid)

    return parser
