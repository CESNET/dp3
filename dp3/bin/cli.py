#!/usr/bin/env python3
"""A utility for running DP3 commands using a CLI."""

import argparse
import sys

from dp3.bin.api import init_parser as init_api_parser
from dp3.bin.api import main as api_main
from dp3.bin.check import init_parser as init_check_parser
from dp3.bin.check import main as check_main
from dp3.bin.config import init_parser as init_config_parser
from dp3.bin.config import main as config_main
from dp3.bin.schema_update import init_parser as init_schema_update_parser
from dp3.bin.schema_update import main as schema_update_main
from dp3.bin.setup import init_parser as init_setup_parser
from dp3.bin.setup import main as setup_main
from dp3.bin.worker import init_parser as init_worker_parser
from dp3.bin.worker import main as worker_main


def init_parser():
    parser = argparse.ArgumentParser(prog="dp3")
    commands = parser.add_subparsers(title="commands", required=True, dest="command")

    worker_parser = commands.add_parser(
        "worker",
        help="Run main worker process of the DP3 platform. ",
        description="Main worker process of the DP3 platform. "
        "There are usually multiple workers running in parallel.",
    )
    init_worker_parser(worker_parser)

    api_parser = commands.add_parser("api", help="Run the DP3 API using uvicorn.")
    init_api_parser(api_parser)

    setup_parser = commands.add_parser("setup", help="Create a DP3 application template.")
    init_setup_parser(setup_parser)

    check_parser = commands.add_parser(
        "check",
        help="Check configuration files for errors.",
        description="Load configuration from given directory and check its validity. "
        "When configuration is OK, program exits immediately with status code 0, "
        "otherwise it prints error messages on stderr and exits with non-zero status.",
    )
    init_check_parser(check_parser)

    config_parser = commands.add_parser(
        "config",
        help="Setup configuration files for a container-less DP3 application deployment.",
        description="Setup configuration files for a container-less DP3 application deployment. ",
    )
    init_config_parser(config_parser)

    schema_update_parser = commands.add_parser(
        "schema-update",
        help="Update the database schema after making conflicting changes to the model.",
        description="Update the database schema after making conflicting changes to the model. ",
    )
    init_schema_update_parser(schema_update_parser)
    return parser


def run():
    parser = init_parser()
    args = parser.parse_args()

    if args.command == "worker":
        worker_main(args)
    elif args.command == "api":
        api_main(args)
    elif args.command == "setup":
        setup_main(args)
    elif args.command == "check":
        check_main(args)
    elif args.command == "config":
        config_main(args)
    elif args.command == "schema-update":
        schema_update_main(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    run()
