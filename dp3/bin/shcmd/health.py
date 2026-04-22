#!/usr/bin/env python3
"""Health commands for the shell-oriented DP3 CLI."""

from dp3.bin.shcmd.common import print_response_json


def handle_health(client, _args) -> int:
    """Check whether the API root responds successfully."""
    return print_response_json(client.request("GET", "/"))


def register_parser(commands) -> None:
    """Register health commands on the root parser."""
    health_parser = commands.add_parser("health", help="Check whether the API is reachable.")
    health_parser.set_defaults(handler=handle_health)
