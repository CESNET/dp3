#!/usr/bin/env python3
"""Simple script to add hashes to master records to allow for easier parallelization.

Intended to be run only once to upgrade a database existing before 08-2023.
"""

import argparse

from pymongo import UpdateOne

from dp3.common.config import ModelSpec, read_config_dir
from dp3.common.task import HASH
from dp3.database.database import EntityDatabase, MongoConfig

# Arguments parser
parser = argparse.ArgumentParser(
    description="Add hashes to master records to allow for easier parallelization in ADiCT."
)
parser.add_argument(
    "--config",
    default="/etc/adict/config",
    help="DP3 config directory (default: /etc/adict/config)",
)
parser.add_argument("-n", default=100, type=int, help="Number of updates to send per request")
args = parser.parse_args()

# Load DP3 configuration
config = read_config_dir(args.config, recursive=True)
model_spec = ModelSpec(config.get("db_entities"))

# Connect to database
connection_conf = MongoConfig.parse_obj(config.get("database", {}))
client = EntityDatabase.connect(connection_conf)
client.admin.command("ping")

db = client[connection_conf.db_name]


def send_updates(entity: str, update_list: list):
    if not update_list:
        return
    res = db[f"{entity}#master"].bulk_write(
        [UpdateOne(_record, {"$set": {"#hash": _hash}}) for _record, _hash in update_list]
    )
    print(res.bulk_api_result)
    update_list.clear()


for entity in model_spec.entities:
    print(entity)
    updates = []
    for record in db[f"{entity}#master"].find({}, projection={"_id": True}):
        updates.append((record, HASH(f"{entity}:{record['_id']}")))
        if len(updates) >= args.n:
            send_updates(entity, updates)
    send_updates(entity, updates)
