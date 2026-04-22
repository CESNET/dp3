#!/usr/bin/env python3
"""Entity commands for the shell-oriented DP3 CLI."""

import argparse
from typing import Optional

from . import etype, instance


def _build_overview_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dp3 sh entity",
        description=(
            "Inspect and modify entity data. Use 'dp3 sh entities' to list entity types, "
            "then continue with either type-scope commands or an entity id."
        ),
    )
    parser.add_argument("selector", nargs="?", metavar="ENTITY_TYPE", help="Entity type.")
    parser.add_argument("rest", nargs=argparse.REMAINDER, metavar="...")
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

    first = args.rest[0]
    if first in etype.TYPE_COMMANDS:
        parsed = etype.build_parser(entity_type).parse_args(args.rest)
    else:
        eid = first
        if len(args.rest) == 1:
            instance.build_parser(entity_type, eid).print_help()
            return None, 2
        parsed = instance.build_parser(entity_type, eid).parse_args(args.rest[1:])

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
    entity_parser.add_argument("selector", nargs="?", metavar="ENTITY_TYPE", help="Entity type.")
    entity_parser.add_argument("rest", nargs=argparse.REMAINDER, metavar="...")
    entity_parser.set_defaults(handler=handle_entity_command, prepare_args=parse_entity_command)
