#!/usr/bin/env python3
"""A script runner entrypoint for DP3 scripts."""
import subprocess
import sys
from argparse import ArgumentParser
from importlib.util import find_spec
from os import chmod
from pathlib import Path


class C:
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKCYAN = "\033[96m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


def red(text: str):
    return f"{C.FAIL}{text}{C.ENDC}"


def yellow(text: str):
    return f"{C.WARNING}{text}{C.ENDC}"


def bold(text: str):
    return f"{C.BOLD}{text}{C.ENDC}"


def try_pandas_import():
    if find_spec("pandas") is None:
        print(
            yellow("WARNING: pandas not found. Some scripts may not work as expected.") + "\n"
            "You can install it with other script dependencies using:\n"
            "  pip install dp-cubed[scripts]\n",
        )


def run():
    parser = ArgumentParser(
        description=f"A script runner entrypoint for {bold('DP3')} scripts.\n"
        "You can enter a script name with arguments, or any of the available commands.\n"
        "Available commands: ls\n",
    )
    parser.add_argument(
        "command",
        help="The command or script to run. Available commands: ls",
    )
    args = parser.parse_args(sys.argv[1:2])
    file_path = Path(__file__)
    script_dir = file_path.parent

    if args.command == "ls":
        for item in sorted(script_dir.iterdir(), key=lambda x: x.name):
            if item.name.startswith("__") or item.name == file_path.name:
                continue
            print(f"{item.name}")
        return
    else:
        script_path = script_dir / args.command
        if not script_path.exists():
            print(red(f"ERROR: Script '{args.command}' not found."))
            return

        try_pandas_import()

        chmod(script_path, 0o775)
        subprocess.run([script_path, *sys.argv[2:]], check=False)
