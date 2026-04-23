#!/usr/bin/env python3
"""Entity attribute-scope commands for the shell-oriented DP3 CLI."""


from dp3.bin.shcmd.common import common_time_params, print_response_json

from .common import (
    add_entity_attr_set_args,
    add_time_range_args,
    complete_entity_attr_names,
    handle_attr_set_request,
)


def handle_get(client, args) -> int:
    """Get one attribute value for one entity."""
    return print_response_json(
        client.request(
            "GET",
            f"/entity/{args.etype}/{args.eid}/get/{args.attr}",
            params=common_time_params(args),
        )
    )


def add_instance_attr_parser(commands, etype: str, eid: str) -> None:
    """Register entity attribute commands under a single-entity parser."""
    attr_parser = commands.add_parser("attr", help="Get or modify an entity attribute value.")
    attr_parser.set_defaults(etype=etype, eid=eid)
    attr_action = attr_parser.add_argument("attr", metavar="ATTR")
    attr_action.completer = complete_entity_attr_names
    attr_commands = attr_parser.add_subparsers(dest="entity_attr_command", required=True)

    get_parser = attr_commands.add_parser("get", help="Get an entity attribute value.")
    add_time_range_args(get_parser)
    get_parser.set_defaults(handler=handle_get, etype=etype, eid=eid)

    set_parser = attr_commands.add_parser("set", help="Set a current entity attribute value.")
    add_entity_attr_set_args(set_parser)
    set_parser.set_defaults(handler=handle_attr_set_request, etype=etype, eid=eid)
