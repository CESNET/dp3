#!/usr/bin/env python3
"""Inspect and fill missing history markers in DP3 master collections.

DP3 uses ``#min_t2s.<attribute>`` to select histories that may contain expired
datapoints. New writers initialize these markers while appending history. Run
this utility once after deploying that behavior to repair records written by
older versions.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pymongo
from pymongo.errors import PyMongoError

from dp3.common.attrspec import AttrType
from dp3.common.config import ModelSpec, read_config_dir
from dp3.common.utils import suppress_dependency_loggers
from dp3.database.config import MongoConfig
from dp3.database.database import MASTER_REVISION_FIELD, EntityDatabase
from dp3.database.encodings import get_codec_options


def init_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Count or fill missing #min_t2s markers in DP3 master collections. "
            "Deploy marker-aware writers before running the fill action."
        )
    )
    parser.add_argument(
        "config_dir",
        metavar="CONFIG_DIR",
        type=Path,
        help="Path to the DP3 configuration directory.",
    )
    parser.add_argument(
        "action",
        choices=("count", "fill"),
        help="Count missing markers without writing, or fill them from stored histories.",
    )
    parser.add_argument(
        "--entity-type",
        action="append",
        dest="entity_types",
        help="Process only this entity type. May be specified more than once.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        help="Logging level. Default: INFO.",
    )
    return parser


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)-15s,%(name)s,[%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    suppress_dependency_loggers()


def connect_to_db(db_config: MongoConfig) -> tuple[pymongo.MongoClient, pymongo.database.Database]:
    client = EntityDatabase.connect(db_config)
    client.admin.command("ping")
    db = client.get_database(db_config.db_name, codec_options=get_codec_options())
    return client, db


def missing_marker_filter(attr_name: str) -> dict:
    """Match a non-empty history whose marker is missing."""
    return {
        f"{attr_name}.0": {"$exists": True},
        f"#min_t2s.{attr_name}": {"$exists": False},
    }


def fill_missing_markers_pipeline(attr_names: list[str]) -> list[dict]:
    """Build one update pipeline that fills missing markers and increments revision once."""
    marker_updates = {}
    for attr_name in attr_names:
        marker_name = f"#min_t2s.{attr_name}"
        marker_ref = f"${marker_name}"
        history = {"$ifNull": [f"${attr_name}", []]}
        marker_updates[marker_name] = {
            "$cond": {
                "if": {"$ne": [{"$type": marker_ref}, "missing"]},
                "then": marker_ref,
                "else": {
                    "$cond": {
                        "if": {"$eq": [{"$size": history}, 0]},
                        "then": "$$REMOVE",
                        "else": {"$min": f"${attr_name}.t2"},
                    }
                },
            }
        }

    marker_updates[MASTER_REVISION_FIELD] = {
        "$add": [{"$ifNull": [f"${MASTER_REVISION_FIELD}", 0]}, 1]
    }
    return [{"$set": marker_updates}]


def history_attributes(model_spec: ModelSpec, entity_type: str) -> list[str]:
    return [
        attr_name
        for attr_name, spec in model_spec.entity_attributes[entity_type].items()
        if spec.t in AttrType.OBSERVATIONS | AttrType.TIMESERIES
    ]


def selected_entities(
    parser: argparse.ArgumentParser, model_spec: ModelSpec, requested: list[str] | None
) -> list[str]:
    if not requested:
        return sorted(model_spec.entities)

    unknown = sorted(set(requested) - set(model_spec.entities))
    if unknown:
        available = ", ".join(sorted(model_spec.entities))
        parser.error(f"Unknown entity type(s) {', '.join(unknown)}. Available: {available}")
    return list(dict.fromkeys(requested))


def inspect_entity(collection, attr_names: list[str]) -> tuple[int, dict[str, int]]:
    counts = {
        attr_name: collection.count_documents(missing_marker_filter(attr_name))
        for attr_name in attr_names
    }
    missing_filters = [missing_marker_filter(attr_name) for attr_name in attr_names]
    documents = collection.count_documents({"$or": missing_filters}) if missing_filters else 0
    return documents, counts


def main(argv: list[str] | None = None) -> int:
    parser = init_parser()
    args = parser.parse_args(argv)
    configure_logging(args.log_level)

    config = read_config_dir(str(args.config_dir), recursive=True)
    model_spec = ModelSpec(config.get("db_entities"))
    entity_types = selected_entities(parser, model_spec, args.entity_types)
    db_config = MongoConfig.model_validate(config.get("database", {}))

    client = None
    try:
        client, db = connect_to_db(db_config)
        collection_names = set(db.list_collection_names())
        total_documents = 0
        total_markers = 0
        remaining_documents = 0
        remaining_markers = 0

        print(f"Action: {args.action}")
        print(f"Database: {db_config.db_name}")

        for entity_type in entity_types:
            attr_names = history_attributes(model_spec, entity_type)
            if not attr_names:
                continue

            collection_name = f"{entity_type}#master"
            if collection_name not in collection_names:
                print(f"{entity_type}: collection does not exist; skipping")
                continue

            collection = db[collection_name]
            missing_documents, counts = inspect_entity(collection, attr_names)
            missing_markers = sum(counts.values())
            total_documents += missing_documents
            total_markers += missing_markers

            print(
                f"{entity_type}: {missing_documents} document(s), "
                f"{missing_markers} missing marker(s)"
            )
            for attr_name, count in counts.items():
                if count:
                    print(f"  {attr_name}: {count}")

            if args.action == "fill":
                if missing_documents:
                    query = {"$or": [missing_marker_filter(attr) for attr in attr_names]}
                    result = collection.update_many(
                        query, fill_missing_markers_pipeline(attr_names)
                    )
                    print(
                        f"  filled: matched={result.matched_count}, "
                        f"modified={result.modified_count}"
                    )
                remaining, remaining_counts = inspect_entity(collection, attr_names)
                remaining_documents += remaining
                remaining_markers += sum(remaining_counts.values())
                print(f"  remaining: {remaining} document(s)")

        if args.action == "count":
            print(f"Total documents with missing markers: {total_documents}")
            print(f"Total missing markers: {total_markers}")
            print("No changes written. Use the fill action to add missing markers.")
        else:
            print(f"Total documents initially missing markers: {total_documents}")
            print(f"Total markers initially missing: {total_markers}")
            print(f"Total documents still missing markers: {remaining_documents}")
            print(f"Total markers still missing: {remaining_markers}")
        return 0
    except PyMongoError as exc:
        print(f"ERROR: MongoDB operation failed: {exc}", file=sys.stderr)
        return 1
    finally:
        if client is not None:
            client.close()


if __name__ == "__main__":
    raise SystemExit(main())
