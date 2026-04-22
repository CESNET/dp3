#!/usr/bin/env python3
"""Datapoint commands for the shell-oriented DP3 CLI."""

from dp3.bin.shcmd.common import print_response_json, read_json_input


def handle_datapoints_ingest(client, args) -> int:
    """Send datapoints from JSON input to the API."""
    body = read_json_input(args.path)
    return print_response_json(client.request("POST", "/datapoints", json_body=body))


def register_parser(commands) -> None:
    """Register datapoint commands on the root parser."""
    datapoints_parser = commands.add_parser("datapoints", help="Send datapoints to the API.")
    datapoints_commands = datapoints_parser.add_subparsers(dest="datapoints_command", required=True)

    ingest_parser = datapoints_commands.add_parser(
        "ingest", help="Post datapoints from JSON input."
    )
    ingest_parser.add_argument(
        "path",
        nargs="?",
        default="-",
        help="Path to a JSON file, or '-' / omitted to read stdin.",
    )
    ingest_parser.set_defaults(handler=handle_datapoints_ingest)
