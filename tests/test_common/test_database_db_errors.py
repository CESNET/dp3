import logging
import unittest
from collections import Counter
from datetime import UTC, datetime
from threading import Lock
from types import SimpleNamespace

from pymongo.errors import AutoReconnect, BulkWriteError

from dp3.database.database import EntityDatabase
from dp3.database.db_errors import DbErrorCounter
from dp3.database.encodings import BSON_MAX_SIZE, get_codec_options
from dp3.database.exceptions import DatabaseError, SnapshotCollectionError
from dp3.database.snapshots import SnapshotCollectionContainer, StringEidSnapshots


class RecordingEventGroup:
    def __init__(self):
        self.counts = Counter()

    def log(self, event_id: str, count: int = 1):
        self.counts[event_id] += count


class FailingCollection:
    def __init__(self, error: Exception):
        self.error = error
        self.updates = []

    def bulk_write(self, updates, ordered=True):
        self.updates.extend(updates)
        raise self.error


def write_error(eid, code, update):
    return {"index": 0, "code": code, "errmsg": "error", "op": {"q": {"_id": eid}, "u": update}}


def make_database(collection, buffered_eids=()):
    database = EntityDatabase.__new__(EntityDatabase)
    database.log = logging.getLogger("test")
    database.elog = RecordingEventGroup()
    database.db_errors = DbErrorCounter(RecordingEventGroup())
    database._master_col = lambda etype, **kwargs: collection
    database._master_buffer_locks = {"ip": Lock()}
    database._master_buffers = {"ip": {eid: {"$set": {"plain": 1}} for eid in buffered_eids}}
    return database


class TestMasterDbErrorCounters(unittest.TestCase):
    def test_push_counts_failed_records_by_cause(self):
        error = BulkWriteError(
            {
                "writeErrors": [
                    write_error("a", 17419, {"$push": {"activity": {}}}),
                    write_error("b", 17419, {"$push": {"activity": {}}}),
                    write_error("c", 2, {"$set": {"plain": 1}}),
                ]
            }
        )
        database = make_database(FailingCollection(error), buffered_eids=["a", "b", "c", "d"])

        with self.assertLogs("test", level="ERROR") as logs:
            database._push_master()

        self.assertEqual(
            database.db_errors.elog.counts,
            {"master_push/ip/document_too_large": 2, "master_push/ip/code_2": 1},
        )
        self.assertIn("ip/a", logs.output[0])
        self.assertIn("['activity']", logs.output[0])

    def test_push_counts_whole_buffer_when_write_fails(self):
        database = make_database(FailingCollection(AutoReconnect()), buffered_eids=["a", "b"])

        with self.assertRaises(DatabaseError):
            database._push_master()

        self.assertEqual(database.db_errors.elog.counts, {"master_push/ip/AutoReconnect": 2})

    def test_update_counts_failed_pipeline_updates(self):
        error = BulkWriteError(
            {
                "nMatched": 1,
                "writeErrors": [write_error("a", 17419, [{"$set": {"history": {}}}])],
            }
        )
        database = make_database(FailingCollection(error))

        with self.assertLogs("test", level="ERROR"):
            matched = database.update_master_records(
                "ip", [("a", 1, None, {"history": []}), ("b", 1, None, {"history": []})]
            )

        self.assertEqual(matched, 1)
        self.assertEqual(
            database.db_errors.elog.counts, {"history_aggregation/ip/document_too_large": 1}
        )

    def test_ttl_update_counts_records_of_failed_write(self):
        database = make_database(FailingCollection(AutoReconnect()))

        with self.assertRaises(DatabaseError):
            database.remove_expired_ttls("ip", {"a": ["data"], "b": ["base"]})

        self.assertEqual(database.db_errors.elog.counts, {"ttl_update/ip/AutoReconnect": 2})


class FailingSnapshots:
    def __init__(self, error: Exception):
        self.error = error

    def save_one(self, snapshot, ctime):
        raise self.error


class RecordingOversizedCollection:
    def __init__(self):
        self.inserted = []

    def insert_many(self, documents):
        self.inserted.extend(documents)


class WritingCollection:
    def __init__(self):
        self.updates = []

    def bulk_write(self, updates, ordered=True):
        self.updates.extend(updates)
        return SimpleNamespace(modified_count=len(updates), upserted_count=0, upserted_ids={})


def make_snapshots(error: Exception | None) -> StringEidSnapshots:
    """Snapshot collection whose normal snapshot bulk write fails with `error`."""
    collection = StringEidSnapshots.__new__(StringEidSnapshots)
    collection.entity_type = "ip"
    collection.log = logging.getLogger("test")
    collection.db_errors = DbErrorCounter(RecordingEventGroup())
    collection._db = SimpleNamespace(codec_options=get_codec_options())
    collection._server_max_bson_size = BSON_MAX_SIZE
    collection._snapshot_bucket_size = 32
    collection._get_state = lambda eids: (sorted(eids), set())  # fixed update order
    collection._snapshot_col = FailingCollection(error)
    collection._oversized_col = RecordingOversizedCollection()
    collection._col = lambda **kwargs: collection._snapshot_col
    collection._os_col = lambda **kwargs: collection._oversized_col
    collection.migrated = []
    collection._migrate_to_oversized = lambda eid, snapshot: collection.migrated.append(snapshot)
    return collection


def indexed_error(index, eid, code):
    return {**write_error(eid, code, {}), "index": index}


class TestSnapshotDbErrorCounters(unittest.TestCase):
    CTIME = datetime(2026, 1, 1, tzinfo=UTC)

    def test_oversized_recovery_completes_when_other_updates_fail(self):
        # A's update is oversized and moved to oversized storage, B's update fails.
        error = BulkWriteError(
            {"writeErrors": [indexed_error(0, "a", 10334), indexed_error(1, "b", 2)]}
        )
        collection = make_snapshots(error)
        snapshots = [{"eid": "a", "n": 1}, {"eid": "a", "n": 2}, {"eid": "b", "n": 1}]

        with self.assertRaises(SnapshotCollectionError):
            collection.save_many(snapshots, self.CTIME)

        self.assertEqual([s["n"] for s in collection.migrated], [2])
        self.assertEqual([s["n"] for s in collection._oversized_col.inserted], [1])
        self.assertEqual(collection.db_errors.elog.counts, {"snapshot_save/ip/code_2": 1})

    def test_write_concern_failure_is_reported_after_oversized_recovery(self):
        error = BulkWriteError(
            {"writeErrors": [indexed_error(0, "a", 10334)], "writeConcernErrors": [{"code": 64}]}
        )
        collection = make_snapshots(error)
        snapshots = [{"eid": "a"}, {"eid": "b"}, {"eid": "c"}]

        with self.assertRaises(SnapshotCollectionError):
            collection.save_many(snapshots, self.CTIME)

        # Updates of B and C are unconfirmed, A's was recovered.
        self.assertEqual(collection.db_errors.elog.counts, {"snapshot_save/ip/unconfirmed": 2})

    def test_failed_move_of_too_large_update_is_counted(self):
        # A's update is too large to send, and moving it to oversized storage fails.
        collection = make_snapshots(None)
        collection._snapshot_col = WritingCollection()
        collection._server_max_bson_size = 1024

        def fail_migration(eid, snapshot):
            raise AutoReconnect()

        collection._migrate_to_oversized = fail_migration
        snapshots = [{"eid": "a", "data": "x" * 2048}, {"eid": "b"}]

        with self.assertRaises(SnapshotCollectionError):
            collection.save_many(snapshots, self.CTIME)

        self.assertEqual(len(collection._snapshot_col.updates), 1)  # B is still saved
        self.assertEqual(collection.db_errors.elog.counts, {"snapshot_save/ip/AutoReconnect": 1})

    def test_save_one_counts_original_exception(self):
        error = SnapshotCollectionError("Insert of snapshot failed")
        error.__cause__ = AutoReconnect()
        container = SnapshotCollectionContainer.__new__(SnapshotCollectionContainer)
        container.db_errors = DbErrorCounter(RecordingEventGroup())
        container._snapshot_collections = {"ip": FailingSnapshots(error)}

        with self.assertRaises(SnapshotCollectionError):
            container.save_one("ip", {"eid": "a"}, self.CTIME)

        self.assertEqual(container.db_errors.elog.counts, {"snapshot_save/ip/AutoReconnect": 1})


class TestWriteConcernFailures(unittest.TestCase):
    def track(self, error, count):
        counter = DbErrorCounter(RecordingEventGroup())
        with self.assertRaises(BulkWriteError), counter.track("master_push", "ip", count):
            raise error
        return counter.elog.counts

    def test_write_concern_failure_counts_writes_as_unconfirmed(self):
        error = BulkWriteError({"writeErrors": [], "writeConcernErrors": [{"code": 64}]})
        self.assertEqual(self.track(error, 5), {"master_push/ip/unconfirmed": 5})

    def test_confirmed_failures_are_counted_apart_from_unconfirmed(self):
        error = BulkWriteError(
            {
                "writeErrors": [write_error("a", 17419, {})],
                "writeConcernErrors": [{"code": 64}],
            }
        )
        self.assertEqual(
            self.track(error, 3),
            {"master_push/ip/document_too_large": 1, "master_push/ip/unconfirmed": 2},
        )


if __name__ == "__main__":
    unittest.main()
