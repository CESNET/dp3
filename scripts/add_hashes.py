#!/usr/bin/env python3
"""Simple script to add hashes to master records to allow for easier parallelization."""

import argparse
import urllib

import pymongo
from pymongo import UpdateOne

from dp3.common.config import ModelSpec, read_config_dir
from dp3.task_processing.task_queue import HASH

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
connection_conf = config.get("database.connection", {})
username = urllib.parse.quote_plus(connection_conf.get("username", "dp3"))
password = urllib.parse.quote_plus(connection_conf.get("password", "dp3"))
address = connection_conf.get("address", "localhost")
port = str(connection_conf.get("port", 27017))
db_name = connection_conf.get("db_name", "dp3")

db = pymongo.MongoClient(
    f"mongodb://{username}:{password}@{address}:{port}/",
    connectTimeoutMS=3000,
    serverSelectionTimeoutMS=5000,
)
db = db[db_name]


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
