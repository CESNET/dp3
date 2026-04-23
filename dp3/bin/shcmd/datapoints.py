#!/usr/bin/env python3
"""Datapoint commands for the shell-oriented DP3 CLI."""

from dp3.bin.shcmd.common import print_response_json, read_json_input


def handle_datapoints(client, args) -> int:
    """Send datapoints from JSON input to the API."""
    body = read_json_input(args.path)
    return print_response_json(client.request("POST", "/datapoints", json_body=body))


def register_parser(commands) -> None:
    """Register datapoint commands on the root parser."""
    datapoints_parser = commands.add_parser(
        "datapoints",
        help="Post datapoints from JSON input.",
        description="Post datapoints from JSON input.",
    )
    datapoints_parser.add_argument(
        "path",
        nargs="?",
        default="-",
        help="Path to a JSON file, or '-' / omitted to read stdin.",
    )
    datapoints_parser.set_defaults(handler=handle_datapoints)
