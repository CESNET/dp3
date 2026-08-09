#!/usr/bin/env python3
"""Telemetry commands for the shell-oriented DP3 CLI."""

import json
import sys

from event_count_logger import EventCountLogger

from dp3.bin.shcmd.common import command_description, print_response_json, stream_json_pages
from dp3.common.config import read_config_dir


def handle_sources_validity(client, _args) -> int:
    """Show source validity timestamps."""
    return print_response_json(client.request("GET", "/telemetry/sources_validity"))


def handle_source_age(client, args) -> int:
    """Show source ages in the requested unit."""
    return print_response_json(
        client.request("GET", "/telemetry/sources_age", params={"unit": args.unit})
    )


def handle_entities_per_attr(client, _args) -> int:
    """Count entities with data present for each attribute."""
    return print_response_json(client.request("GET", "/telemetry/entities_per_attr"))


def handle_attribute_bson_sizes(client, _args) -> int:
    """Show cached logical BSON-size statistics for configured attributes."""
    return print_response_json(client.request("GET", "/telemetry/attribute_bson_sizes"))


def handle_snapshot_summary(client, _args) -> int:
    """Show recent snapshot activity summary."""
    return print_response_json(client.request("GET", "/telemetry/snapshot_summary"))


def handle_metadata(client, args) -> int:
    """Browse internal metadata records."""
    params = {
        "skip": args.skip,
        "limit": args.limit,
        "sort": args.sort,
    }
    if args.module is not None:
        params["module"] = args.module
    if args.date_from is not None:
        params["date_from"] = args.date_from
    if args.date_to is not None:
        params["date_to"] = args.date_to

    if args.format == "ndjson":
        base_params = {k: v for k, v in params.items() if k not in {"skip", "limit"}}
        return stream_json_pages(client, "/telemetry/metadata", base_params, args.skip, args.limit)
    return print_response_json(client.request("GET", "/telemetry/metadata", params=params))


def handle_rabbitmq_queues(client, _args) -> int:
    """Show RabbitMQ queue telemetry."""
    return print_response_json(client.request("GET", "/telemetry/rabbitmq/queues"))


def handle_event_counts(_client, args) -> int:
    """Read EventCountLogger counters from Redis."""
    try:
        config = read_config_dir(args.config_dir, recursive=True)
        groups = config.get("event_logging.groups")
        redis_config = config.get("event_logging.redis")
        group_config = groups.get(args.group) if isinstance(groups, dict) else None
        if group_config is None:
            raise ValueError(f"Event counter group '{args.group}' is not configured")

        intervals = group_config.get("intervals", [])
        if args.interval not in intervals:
            configured = ", ".join(intervals) or "none"
            raise ValueError(
                f"Interval '{args.interval}' is not configured for group "
                f"'{args.group}' (configured: {configured})"
            )

        group = EventCountLogger(groups, redis_config).get_group(args.group)
        result = {"group": args.group, "interval": args.interval}
        if not args.current:
            result["last"] = group.get_counts(args.interval)
        if args.current or args.both:
            result["current"] = group.get_counts(args.interval, current=True)
    except Exception as e:
        print(f"Cannot read event counters: {e}", file=sys.stderr)
        return 1

    json.dump(result, sys.stdout, sort_keys=True)
    sys.stdout.write("\n")
    return 0


def register_parser(commands) -> None:
    """Register telemetry commands on the root parser."""
    telemetry_parser = commands.add_parser(
        "telemetry",
        help="Read operational telemetry.",
        description="Read operational telemetry from DP³ services.",
    )
    telemetry_commands = telemetry_parser.add_subparsers(dest="telemetry_command", required=True)

    sources_validity_parser = telemetry_commands.add_parser(
        "sources-validity",
        help="Show source validity timestamps.",
        description="Show the latest datapoint validity timestamp observed for each source.",
    )
    sources_validity_parser.set_defaults(handler=handle_sources_validity)

    source_age_parser = telemetry_commands.add_parser(
        "source-age",
        help="Show source ages.",
        description="Show the age of each source in the selected unit.",
    )
    source_age_parser.add_argument(
        "-u",
        "--unit",
        choices=["minutes", "seconds"],
        default="minutes",
    )
    source_age_parser.set_defaults(handler=handle_source_age)

    entities_per_attr_parser = telemetry_commands.add_parser(
        "entities-per-attr",
        help="Count entities with data present for each attribute.",
        description="Count entities with data present for each configured attribute.",
    )
    entities_per_attr_parser.set_defaults(handler=handle_entities_per_attr)

    attribute_bson_sizes_parser = telemetry_commands.add_parser(
        "attribute-bson-sizes",
        help="Show cached logical BSON-size statistics for attributes.",
        description="Show cached logical BSON-size statistics for configured attributes.",
    )
    attribute_bson_sizes_parser.set_defaults(handler=handle_attribute_bson_sizes)

    snapshot_summary_parser = telemetry_commands.add_parser(
        "snapshot-summary",
        help="Show recent snapshot activity summary.",
        description="Show a summary of recent snapshot activity.",
    )
    snapshot_summary_parser.set_defaults(handler=handle_snapshot_summary)

    metadata_parser = telemetry_commands.add_parser(
        "metadata",
        help="Browse internal metadata records.",
        description=command_description(
            "Browse diagnostic records produced by internal periodic processes. Time bounds "
            "are ISO 8601 timestamps.",
            "dp3 sh telemetry metadata --module SnapShooter "
            "--from 2024-01-01T00:00:00Z --sort oldest --limit 100 --format ndjson",
        ),
    )
    metadata_parser.add_argument("-m", "--module", help="Limit records to one module.")
    metadata_parser.add_argument(
        "-f", "--from", dest="date_from", help="ISO 8601 lower timestamp bound."
    )
    metadata_parser.add_argument(
        "-t", "--to", dest="date_to", help="ISO 8601 upper timestamp bound."
    )
    metadata_parser.add_argument(
        "-s", "--skip", type=int, default=0, help="Skip this many records."
    )
    metadata_parser.add_argument(
        "-l", "--limit", type=int, default=0, help="Return at most this many records."
    )
    metadata_parser.add_argument(
        "-S",
        "--sort",
        choices=["newest", "oldest"],
        default="newest",
        help="Select record ordering.",
    )
    metadata_parser.add_argument(
        "-F",
        "--format",
        choices=["json", "ndjson"],
        default="ndjson",
        help="Choose JSON or NDJSON output.",
    )
    metadata_parser.set_defaults(handler=handle_metadata)

    rabbitmq_queues_parser = telemetry_commands.add_parser(
        "rabbitmq-queues",
        help="Show RabbitMQ queue telemetry.",
        description="Show queue sizes, consumers, and message rates for the application.",
    )
    rabbitmq_queues_parser.set_defaults(handler=handle_rabbitmq_queues)

    event_counts_parser = telemetry_commands.add_parser(
        "event-counts",
        help="Read EventCountLogger counters from Redis.",
        description=command_description(
            "Read EventCountLogger counters directly from the configured Redis instance.",
            "dp3 sh telemetry event-counts --group te --interval 5m --both",
        ),
    )
    event_counts_parser.add_argument(
        "-g", "--group", required=True, help="Configured event counter group."
    )
    event_counts_parser.add_argument(
        "-i", "--interval", required=True, help="Configured counter interval."
    )
    counter_period = event_counts_parser.add_mutually_exclusive_group()
    counter_period.add_argument(
        "--last",
        action="store_true",
        help="Show the last completed interval (default).",
    )
    counter_period.add_argument(
        "--current",
        action="store_true",
        help="Show the current incomplete interval.",
    )
    counter_period.add_argument(
        "--both",
        action="store_true",
        help="Show both the last and current intervals.",
    )
    event_counts_parser.set_defaults(
        handler=handle_event_counts,
        requires_api=False,
        load_model_spec=False,
    )
