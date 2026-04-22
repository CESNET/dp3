#!/usr/bin/env python3
"""Control commands for the shell-oriented DP3 CLI."""

from dp3.bin.shcmd.common import complete_entity_type_names, print_response_json


def handle_make_snapshots(client, _args) -> int:
    """Trigger an out-of-order snapshot run."""
    return print_response_json(client.request("GET", "/control/make_snapshots"))


def handle_refresh_on_entity_creation(client, args) -> int:
    """Re-run entity-creation callbacks for an entity type."""
    return print_response_json(
        client.request(
            "GET",
            "/control/refresh_on_entity_creation",
            params={"etype": args.etype},
        )
    )


def handle_refresh_module_config(client, args) -> int:
    """Reload configuration for one module."""
    return print_response_json(
        client.request(
            "GET",
            "/control/refresh_module_config",
            params={"module": args.module},
        )
    )


def register_parser(commands) -> None:
    """Register control commands on the root parser."""
    control_parser = commands.add_parser("control", help="Execute control actions.")
    control_commands = control_parser.add_subparsers(dest="control_command", required=True)

    make_snapshots_parser = control_commands.add_parser(
        "make-snapshots", help="Trigger an out-of-order snapshot run."
    )
    make_snapshots_parser.set_defaults(handler=handle_make_snapshots)

    refresh_entity_creation_parser = control_commands.add_parser(
        "refresh-on-entity-creation",
        help="Re-run entity creation callbacks for an entity type.",
    )
    etype_action = refresh_entity_creation_parser.add_argument("etype")
    etype_action.completer = complete_entity_type_names
    refresh_entity_creation_parser.set_defaults(handler=handle_refresh_on_entity_creation)

    refresh_module_config_parser = control_commands.add_parser(
        "refresh-module-config",
        help="Reload module configuration.",
    )
    refresh_module_config_parser.add_argument("module")
    refresh_module_config_parser.set_defaults(handler=handle_refresh_module_config)
