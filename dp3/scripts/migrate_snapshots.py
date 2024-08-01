#!/usr/bin/env python3
"""Simple script to migrate snapshots schema from flat single-snapshot documents to
a nested history-last schema.

Intended to be run only once to upgrade a database existing before 08-2024.
"""

import argparse
from collections import defaultdict

import bson
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError, DocumentTooLarge, OperationFailure, WriteError

from dp3.common.config import ModelSpec, read_config_dir
from dp3.database.database import DatabaseError, EntityDatabase, MongoConfig

# Arguments parser
parser = argparse.ArgumentParser(
    description="Migrate snapshots schema from flat single-snapshot documents "
    "to a nested history schema."
)
parser.add_argument(
    "--config",
    default="/etc/adict/config",
    help="DP3 config directory (default: /etc/adict/config)",
)
parser.add_argument("--dry-run", action="store_true", help="Do not write to database")
parser.add_argument("-n", default=100, type=int, help="Number of updates to send per request")
args = parser.parse_args()

# Load DP3 configuration
config = read_config_dir(args.config, recursive=True)
model_spec = ModelSpec(config.get("db_entities"))

# Connect to database
connection_conf = MongoConfig.model_validate(config.get("database", {}))
client = EntityDatabase.connect(connection_conf)
print(client)
client.admin.command("ping")

db = client[connection_conf.db_name]

BSON_OBJECT_TOO_LARGE = 10334


def _migrate_to_oversized_snapshot(etype: str, eid: str, snapshot: dict):
    snapshot_col = f"{etype}#snapshots"
    os_col = f"{etype}#snapshots_oversized"

    if "_id" in snapshot:
        print(f"Removing _id {snapshot['_id']} from snapshot of {eid}")
        del snapshot["_id"]

    try:
        doc = db[snapshot_col].find_one_and_update(
            {"_id": eid},
            {
                "$set": {"oversized": True, "last": snapshot, "count": 0},
                "$unset": {"history": ""},
            },
        )
        inserts = list(doc.get("history", []))
        inserts.insert(0, doc.get("last", {}))
        inserts.insert(0, snapshot)
        db[os_col].insert_many(inserts)
    except Exception as e:
        raise DatabaseError(f"Update of snapshot {eid} failed: {e}, {snapshot}") from e


def save_snapshot(etype: str, snapshot: dict):
    """Saves snapshot to specified entity of current master document.

    Will move snapshot to oversized snapshots if the maintained bucket is too large.
    """
    if "eid" not in snapshot:
        return
    eid = snapshot["eid"]

    if "_id" in snapshot:
        print(f"Removing _id {snapshot['_id']} from snapshot of {eid}")
        del snapshot["_id"]

    snapshot_col = f"{etype}#snapshots"
    os_col = f"{etype}#snapshots_oversized"

    # Find out if the snapshot is oversized
    doc = db[snapshot_col].find_one({"_id": eid}, {"oversized": 1})
    if doc is None:
        # First snapshot of entity
        db[snapshot_col].insert_one(
            {"_id": eid, "last": snapshot, "history": [], "oversized": False, "count": 0}
        )
        print(f"Inserted snapshot of {eid}")
        return
    elif doc.get("oversized", False):
        # Snapshot is already marked as oversized
        db[snapshot_col].update_one({"_id": eid}, {"$set": {"last": snapshot}})
        db[os_col].insert_one(snapshot)
        return

    try:
        # Update a normal snapshot bucket
        res = db[snapshot_col].update_one(
            {"_id": eid},
            [
                {
                    "$set": {
                        "history": {
                            "$concatArrays": [
                                ["$last"],
                                "$history",
                            ]
                        },
                        "count": {"$sum": ["$count", 1]},
                    }
                },
                {"$set": {"last": {"$literal": snapshot}}},
            ],
        )
        if res.modified_count == 0:
            print(f"Snapshot of {eid} was not updated, {res.raw_result}")
    except (WriteError, OperationFailure) as e:
        if e.code != BSON_OBJECT_TOO_LARGE:
            raise e
        # The snapshot is too large, move it to oversized snapshots
        print(f"Snapshot of {eid} is too large: {e}, marking as oversized.")
        _migrate_to_oversized_snapshot(etype, eid, snapshot)
    except Exception as e:
        raise DatabaseError(f"Insert of snapshot {eid} failed: {e}, {snapshot}") from e


def save_snapshots(etype: str, snapshots: list[dict]):
    """
    Saves a list of snapshots of current master documents.

    All snapshots must belong to same entity type.

    Will move snapshots to oversized snapshots if the maintained bucket is too large.
    For better understanding, see `save_snapshot()`.

    """
    snapshots_by_eid = defaultdict(list)
    for snapshot in snapshots:
        if "eid" not in snapshot:
            continue
        if "_id" in snapshot:
            del snapshot["_id"]
        snapshots_by_eid[snapshot["eid"]].append(snapshot)
    print(f"Saving {len(snapshots)} snapshots of {len(snapshots_by_eid)} entities of {etype}")

    snapshot_col = f"{etype}#snapshots"
    os_col = f"{etype}#snapshots_oversized"

    # Find out if any of the snapshots are oversized
    docs = list(
        db[snapshot_col].find(
            {"_id": {"$in": list(snapshots_by_eid.keys())}}, {"oversized": 1, "eid": 1}
        )
    )

    updates = []
    update_originals = []
    oversized_inserts = []
    oversized_updates = []

    for doc in docs:
        eid = doc["_id"]
        if not doc.get("oversized", False):
            # A normal snapshot, shift the last snapshot to history and update last
            updates.append(
                UpdateOne(
                    {"_id": eid},
                    [
                        {
                            "$set": {
                                "history": {
                                    "$concatArrays": [
                                        snapshots_by_eid[eid][:-1],
                                        ["$last"],
                                        "$history",
                                    ]
                                },
                                "count": {"$sum": [len(snapshots_by_eid[eid]), "$count"]},
                            }
                        },
                        {"$set": {"last": {"$literal": snapshots_by_eid[eid][-1]}}},
                    ],
                )
            )
            update_originals.append(snapshots_by_eid[eid])
        else:
            # Snapshot is already marked as oversized
            oversized_inserts.extend(snapshots_by_eid[eid])
            oversized_updates.append(
                UpdateOne({"_id": eid}, {"$set": {"last": snapshots_by_eid[eid][-1]}})
            )
        del snapshots_by_eid[eid]

    # The remaining snapshots are new
    inserts = [
        {
            "_id": eid,
            "last": eid_snapshots[-1],
            "history": eid_snapshots[:-1],
            "oversized": False,
            "count": len(eid_snapshots) - 1,
        }
        for eid, eid_snapshots in snapshots_by_eid.items()
    ]

    if updates:
        try:
            res = db[snapshot_col].bulk_write(updates, ordered=False)
            if res.modified_count != len(updates):
                print(
                    f"Some snapshots were not updated, "
                    f"{res.modified_count} != {len(snapshots_by_eid)}"
                )
        except (BulkWriteError, OperationFailure) as e:
            print("Update of snapshots failed, will retry with oversize.")
            failed_indexes = [
                err["index"]
                for err in e.details["writeErrors"]
                if err["code"] == BSON_OBJECT_TOO_LARGE
            ]
            failed_snapshots = (update_originals[i] for i in failed_indexes)
            for eid_snapshots in failed_snapshots:
                eid = eid_snapshots[0]["eid"]
                failed_snapshots = sorted(
                    eid_snapshots, key=lambda s: s["_time_created"], reverse=True
                )
                _migrate_to_oversized_snapshot(etype, eid, failed_snapshots[0])
                oversized_inserts.extend(failed_snapshots[1:])

            if any(err["code"] != BSON_OBJECT_TOO_LARGE for err in e.details["writeErrors"]):
                # Some other error occurred
                raise e
        except Exception as e:
            raise DatabaseError(f"Update of snapshots failed: {str(e)[:2048]}") from e

    if inserts:
        try:
            # Insert new snapshots
            res = db[snapshot_col].insert_many(inserts, ordered=False)
            if len(res.inserted_ids) != len(snapshots_by_eid):
                print(
                    f"Some snapshots were not inserted, "
                    f"{len(res.inserted_ids)} != {len(snapshots_by_eid)}"
                )
        except (DocumentTooLarge, OperationFailure) as e:
            print(f"Snapshot too large: {e}")
            checked_inserts = []
            oversized_inserts = []

            # Filter out the oversized snapshots
            for insert_doc in inserts:
                bsize = len(bson.BSON.encode(insert_doc))
                if bsize < 16 * 1024 * 1024:
                    checked_inserts.append(insert_doc)
                else:
                    eid = insert_doc["_id"]
                    checked_inserts.append(
                        {
                            "_id": eid,
                            "last": insert_doc["last"],
                            "oversized": True,
                            "history": [],
                            "count": 0,
                        }
                    )
                    oversized_inserts.extend(insert_doc["history"] + [insert_doc["last"]])
            try:
                db[snapshot_col].insert_many(checked_inserts, ordered=False)
            except Exception as e:
                raise DatabaseError(f"Insert of snapshots failed: {e}") from e
        except Exception as e:
            raise DatabaseError(f"Insert of snapshot failed: {e}") from e

    # Update the oversized snapshots
    if oversized_inserts:
        try:
            if oversized_updates:
                db[snapshot_col].bulk_write(oversized_updates)
            db[os_col].insert_many(oversized_inserts)
        except Exception as e:
            raise DatabaseError(f"Insert of snapshots failed: {str(e)[:2048]}") from e


for entity in model_spec.entities:
    if len(list(db.list_collections(filter={"name": f"{entity}#snapshots_old"}))) == 0:
        db[f"{entity}#snapshots"].rename(f"{entity}#snapshots_old")
    print(entity)

    snapshots = []
    for record in db[f"{entity}#snapshots_old"].find({}, sort=[("_time_created", 1)]):
        del record["_id"]
        snapshots.append(record)
        if len(snapshots) >= args.n:
            if not args.dry_run:
                save_snapshots(entity, snapshots)
            else:
                print(f"Would save {len(snapshots)} snapshots")
            snapshots.clear()
    if snapshots and not args.dry_run:
        save_snapshots(entity, snapshots)
    elif snapshots:
        print(f"Would save {len(snapshots)} snapshots")
