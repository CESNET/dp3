#!/usr/bin/env python3
"""Shell-oriented CLI for interacting with a running DP3 API."""

import argparse
import sys

from dp3.bin.shcmd import control, datapoints, entities, entity, health, telemetry
from dp3.bin.shcmd.common import APIError, DP3APIClient, resolve_config_dir
from dp3.common.config import ModelSpec, read_config_dir


def init_parser(parser: argparse.ArgumentParser) -> None:
    """Initialize the shell-oriented CLI parser."""
    parser.add_argument(
        "--config",
        default=None,
        help=(
            "Path to the DP3 configuration directory. "
            "Resolution order: --config, DP3_CONFIG_DIR, ./config."
        ),
    )
    parser.add_argument(
        "--url",
        default=None,
        help="Base URL of the DP3 API. When omitted, localhost defaults are probed.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        help="HTTP timeout in seconds.",
    )

    commands = parser.add_subparsers(dest="sh_command", required=True)
    health.register_parser(commands)
    datapoints.register_parser(commands)
    entities.register_parser(commands)
    entity.register_parser(commands)
    control.register_parser(commands)
    telemetry.register_parser(commands)


def run() -> None:
    """Run the shell-oriented CLI as a standalone script."""
    parser = argparse.ArgumentParser(prog="dp3 sh")
    init_parser(parser)
    args = parser.parse_args()
    sys.exit(main(args))


def main(args) -> int:
    """Execute a parsed shell-oriented CLI command."""
    parsed_args = args
    prepare_args = getattr(args, "prepare_args", None)
    if prepare_args is not None:
        parsed_args, exit_code = prepare_args(args)
        if exit_code is not None:
            return exit_code

    config_dir = resolve_config_dir(args.config)
    try:
        config = read_config_dir(config_dir, recursive=True)
        model_spec = ModelSpec(config.get("db_entities"))
    except Exception as e:
        print(f"Cannot read config directory '{config_dir}': {e}", file=sys.stderr)
        return 1

    try:
        client = DP3APIClient(config_dir, args.url, args.timeout, model_spec=model_spec)
        return parsed_args.handler(client, parsed_args)
    except APIError as e:
        print(str(e), file=sys.stderr)
        return 1


if __name__ == "__main__":
    run()
