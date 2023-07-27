#!/usr/bin/env python3
import argparse
import sys

from dp3 import worker


def init_parser(parser):
    parser.add_argument(
        "app_name",
        metavar="APP_NAME",
        help="Name of the application to distinct it from other DP3-based apps "
        "(it's used as a prefix of RabbitMQ queue names, for example).",
    )
    parser.add_argument(
        "config_dir",
        metavar="CONFIG_DIRECTORY",
        help="Path to a directory containing configuration files (e.g. /etc/my_app/config)",
    )
    parser.add_argument(
        "process_index",
        metavar="PROCESS_INDEX",
        type=int,
        help="Index of this worker process. For each application there must be N processes running "
        "simultaneously, each started with a unique index (from 0 to N-1). N is read from "
        "configuration ('worker_processes' in 'processing_core.yml').",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=False,
        help="More verbose output (set log level to DEBUG).",
    )


def run():
    print(
        "WARNING: The `worker` entrypoint is deprecated due to possible namespace conflicts. "
        "Please use `dp3 worker` instead.",
        file=sys.stderr,
    )

    # Parse arguments
    parser = argparse.ArgumentParser(
        prog="worker",
        description="Main worker process of the DP3 platform. "
        "There are usually multiple workers running in parallel.",
    )
    init_parser(parser)
    args = parser.parse_args()

    main(args)


def main(args):
    worker.main(args.app_name, args.config_dir, args.process_index, args.verbose)


if __name__ == "__main__":
    run()
