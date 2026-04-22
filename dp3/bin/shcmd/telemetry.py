#!/usr/bin/env python3
"""Telemetry commands for the shell-oriented DP3 CLI."""

from dp3.bin.shcmd.common import print_response_json, stream_json_pages


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


def register_parser(commands) -> None:
    """Register telemetry commands on the root parser."""
    telemetry_parser = commands.add_parser("telemetry", help="Read operational telemetry.")
    telemetry_commands = telemetry_parser.add_subparsers(dest="telemetry_command", required=True)

    sources_validity_parser = telemetry_commands.add_parser(
        "sources-validity", help="Show source validity timestamps."
    )
    sources_validity_parser.set_defaults(handler=handle_sources_validity)

    source_age_parser = telemetry_commands.add_parser("source-age", help="Show source ages.")
    source_age_parser.add_argument("--unit", choices=["minutes", "seconds"], default="minutes")
    source_age_parser.set_defaults(handler=handle_source_age)

    entities_per_attr_parser = telemetry_commands.add_parser(
        "entities-per-attr", help="Count entities with data present for each attribute."
    )
    entities_per_attr_parser.set_defaults(handler=handle_entities_per_attr)

    snapshot_summary_parser = telemetry_commands.add_parser(
        "snapshot-summary", help="Show recent snapshot activity summary."
    )
    snapshot_summary_parser.set_defaults(handler=handle_snapshot_summary)

    metadata_parser = telemetry_commands.add_parser(
        "metadata", help="Browse internal metadata records."
    )
    metadata_parser.add_argument("--module")
    metadata_parser.add_argument("--from", dest="date_from")
    metadata_parser.add_argument("--to", dest="date_to")
    metadata_parser.add_argument("--skip", type=int, default=0)
    metadata_parser.add_argument("--limit", type=int, default=0)
    metadata_parser.add_argument("--sort", choices=["newest", "oldest"], default="newest")
    metadata_parser.add_argument("--format", choices=["json", "ndjson"], default="json")
    metadata_parser.set_defaults(handler=handle_metadata)

    rabbitmq_queues_parser = telemetry_commands.add_parser(
        "rabbitmq-queues", help="Show RabbitMQ queue telemetry."
    )
    rabbitmq_queues_parser.set_defaults(handler=handle_rabbitmq_queues)
