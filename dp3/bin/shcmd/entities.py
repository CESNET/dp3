#!/usr/bin/env python3
"""Entity catalog commands for the shell-oriented DP3 CLI."""

from dp3.bin.shcmd.common import print_response_json


def handle_entities(client, _args) -> int:
    """Return the full entity-type map exposed by the API."""
    return print_response_json(client.request("GET", "/entities"))


def register_parser(commands) -> None:
    """Register entity catalog commands on the root parser."""
    entities_parser = commands.add_parser(
        "entities",
        help="Return the full entity-type map exposed by the API.",
        description=(
            "Return the full entity-type map exposed by the API. To print only entity "
            "type names, use 'dp3 sh entities | jq keys' or '<APPNAME>sh entities | jq "
            "keys'."
        ),
    )
    entities_parser.set_defaults(handler=handle_entities)
