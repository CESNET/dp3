#!/usr/bin/env python3
"""Entity commands for the shell-oriented DP3 CLI."""

import argparse
from typing import Optional

from . import etype
from .common import complete_entity_rest, complete_entity_selector


def _build_overview_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dp3 sh entity",
        description=(
            "Inspect and modify entity data. Use 'dp3 sh entities' to list entity types, "
            "then continue with a type-scope command or 'id' for a single entity."
        ),
    )
    selector_action = parser.add_argument(
        "selector", nargs="?", metavar="ENTITY_TYPE", help="Entity type."
    )
    selector_action.completer = complete_entity_selector
    rest_action = parser.add_argument("rest", nargs=argparse.REMAINDER, metavar="...")
    rest_action.completer = complete_entity_rest
    return parser


def parse_entity_command(args) -> tuple[Optional[argparse.Namespace], Optional[int]]:
    """Parse the path-like entity command grammar."""
    overview_parser = _build_overview_parser()
    if args.selector is None:
        overview_parser.print_help()
        return None, 2

    entity_type = args.selector
    if not args.rest:
        etype.build_parser(entity_type).print_help()
        return None, 2

    parsed = etype.build_parser(entity_type).parse_args(args.rest)
    return parsed, None


def handle_entity_command(_client, _args) -> int:
    """Placeholder for the top-level entity parser before nested parsing runs."""
    raise RuntimeError("Entity commands must be parsed before execution.")


def register_parser(commands) -> None:
    """Register entity commands on the root parser."""
    overview_parser = _build_overview_parser()
    entity_parser = commands.add_parser(
        "entity",
        help="Inspect and modify entity data.",
        description=overview_parser.description,
    )
    selector_action = entity_parser.add_argument(
        "selector", nargs="?", metavar="ENTITY_TYPE", help="Entity type."
    )
    selector_action.completer = complete_entity_selector
    rest_action = entity_parser.add_argument("rest", nargs=argparse.REMAINDER, metavar="...")
    rest_action.completer = complete_entity_rest
    entity_parser.set_defaults(handler=handle_entity_command, prepare_args=parse_entity_command)
