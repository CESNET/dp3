#!/usr/bin/env python3
"""Simple script to add min_t2s to master records to allow for better indexing.

Intended to be run only once to upgrade a database existing before 08-2024.
"""

import argparse
import time

from dp3.common.attrspec import AttrType
from dp3.common.config import ModelSpec, read_config_dir
from dp3.database.database import EntityDatabase, MongoConfig

# Arguments parser
parser = argparse.ArgumentParser(
    description="Add min_t2s to master records to allow for better indexing."
)
parser.add_argument(
    "--config",
    default="/etc/adict/config",
    help="DP3 config directory (default: /etc/adict/config)",
)
args = parser.parse_args()

# Load DP3 configuration
config = read_config_dir(args.config, recursive=True)
model_spec = ModelSpec(config.get("db_entities"))

# Connect to database
connection_conf = MongoConfig.model_validate(config.get("database", {}))
client = EntityDatabase.connect(connection_conf)
client.admin.command("ping")

db = client[connection_conf.db_name]


for entity, attributes in model_spec.entity_attributes.items():
    t1 = time.time()
    res = db[f"{entity}#master"].update_many(
        {},
        [
            {
                "$set": {
                    attr_name: {
                        "$cond": {
                            "if": {
                                "$eq": [
                                    {"$size": {"$ifNull": [f"${attr_name}", []]}},
                                    0,
                                ]
                            },
                            "then": "$$REMOVE",
                            "else": f"${attr_name}",
                        }
                    }
                    for attr_name, spec in attributes.items()
                    if spec.t != AttrType.PLAIN
                }
                | {
                    f"#min_t2s.{attr_name}": {
                        "$cond": {
                            "if": {
                                "$eq": [
                                    {"$size": {"$ifNull": [f"${attr_name}", []]}},
                                    0,
                                ]
                            },
                            "then": "$$REMOVE",
                            "else": {"$min": f"${attr_name}.t2"},
                        }
                    }
                    for attr_name, spec in attributes.items()
                    if spec.t != AttrType.PLAIN
                }
            },
        ],
    )
    t2 = time.time()
    print(f"Updated {res.modified_count} records for entity {entity} in {t2 - t1:.2f}s")
