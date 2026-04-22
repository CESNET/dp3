#!/usr/bin/env python3
"""Shell-oriented CLI for interacting with a running DP3 API."""

import argparse
import sys

import argcomplete
from argcomplete.completers import DirectoriesCompleter
from argcomplete.shell_integration import shellcode

from dp3.bin.shcmd import control, datapoints, entities, entity, health, telemetry
from dp3.bin.shcmd.common import APIError, DP3APIClient, resolve_config_dir
from dp3.common.config import ModelSpec, read_config_dir


def render_completion_shellcode(shell_name: str, command_names: list[str]) -> str:
    """Render shell completion registration code using argcomplete."""
    commands = command_names or ["dp3"]
    return shellcode(commands, shell=shell_name)


def handle_completion(_client, args) -> int:
    """Print shell completion registration code."""
    sys.stdout.write(render_completion_shellcode(args.shell_name, args.command))
    if not sys.stdout.isatty():
        sys.stdout.write("\n")
    return 0


def register_completion_parser(commands) -> None:
    """Register shell completion helpers on the root parser."""
    completion_parser = commands.add_parser(
        "completion",
        help="Print shell completion scripts.",
        description=(
            "Print shell completion scripts backed by argcomplete. Evaluate the generated "
            "output in your shell or source it from your shell startup file."
        ),
    )
    completion_commands = completion_parser.add_subparsers(dest="shell_name", required=True)
    for shell_name in ["bash", "zsh", "fish"]:
        shell_parser = completion_commands.add_parser(
            shell_name, help=f"Print a {shell_name.capitalize()} completion script."
        )
        shell_parser.add_argument(
            "--command",
            action="append",
            default=[],
            help=(
                "Command name to register completion for. Repeat to register multiple "
                "commands. Use 'dp3' for 'dp3 sh' and '<APPNAME>sh' for wrapper commands."
            ),
        )
        shell_parser.set_defaults(
            handler=handle_completion, requires_api=False, load_model_spec=False
        )


def init_parser(parser: argparse.ArgumentParser) -> None:
    """Initialize the shell-oriented CLI parser."""
    config_action = parser.add_argument(
        "--config",
        default=None,
        help=(
            "Path to the DP3 configuration directory. "
            "Resolution order: --config, DP3_CONFIG_DIR, ./config."
        ),
    )
    config_action.completer = DirectoriesCompleter()
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
    register_completion_parser(commands)


def run() -> None:
    """Run the shell-oriented CLI as a standalone script."""
    parser = argparse.ArgumentParser(prog="dp3 sh")
    init_parser(parser)
    argcomplete.autocomplete(parser)
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
    parsed_args.config_dir = config_dir

    model_spec = None
    if getattr(parsed_args, "load_model_spec", True):
        try:
            config = read_config_dir(config_dir, recursive=True)
            model_spec = ModelSpec(config.get("db_entities"))
        except Exception as e:
            if not getattr(parsed_args, "ignore_config_errors", False):
                print(f"Cannot read config directory '{config_dir}': {e}", file=sys.stderr)
                return 1
    parsed_args.model_spec = model_spec

    if not getattr(parsed_args, "requires_api", True):
        return parsed_args.handler(None, parsed_args)

    try:
        client = DP3APIClient(config_dir, args.url, args.timeout, model_spec=model_spec)
        return parsed_args.handler(client, parsed_args)
    except APIError as e:
        print(str(e), file=sys.stderr)
        return 1


if __name__ == "__main__":
    run()
