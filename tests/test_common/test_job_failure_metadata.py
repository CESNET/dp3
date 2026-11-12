"""A failed periodic job still records its end in `#metadata`, with the error.

Without the end record, a failed run can't be told apart from a run interrupted
by a worker restart.
"""

import logging
import os
import unittest
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import Mock, patch

from dp3.common.attrspec import AttrType
from dp3.common.config import ModelSpec, read_config_dir
from dp3.core.collector import GarbageCollector
from dp3.database.database import DatabaseError, EntityDatabase
from dp3.history_management.history_manager import HistoryManager
from dp3.snapshots.snapshooter import SnapShooter

METADATA_LOG = "EntityDatabase.Metadata"


class JobFailure(RuntimeError):
    pass


class FailingCursor:
    """Yields the given documents, then fails."""

    def __init__(self, docs):
        self.docs = docs

    def __iter__(self):
        yield from self.docs
        raise JobFailure("boom")

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        pass


class FailedJobEndRecord(unittest.TestCase):
    def setUp(self):
        self.metadata = Mock()
        self.metadata.update_one.return_value = Mock(matched_count=1)
        self.db = EntityDatabase.bare(SimpleNamespace(entities={}), {"#metadata": self.metadata})
        self.db.get_worker_master_records = Mock(side_effect=JobFailure("boom"))
        self.db.delete_eids = Mock()

    def end_record(self, module: str, end_field: str) -> tuple[dict, dict]:
        """Returns the fields set and increased by the end record, the last metadata update."""
        query, changes = self.metadata.update_one.call_args.args
        self.assertRegex(query["_id"], f"^{module}\\d")
        self.assertIn(end_field, changes["$set"])
        return changes["$set"], changes["$inc"]

    def make_collector(self) -> GarbageCollector:
        collector = GarbageCollector.__new__(GarbageCollector)
        collector.log = logging.getLogger(self.id())
        collector.db = self.db
        collector.worker_index, collector.num_workers = 0, 1
        collector.model_spec = SimpleNamespace(entities={})
        collector.cache = Mock()
        collector.cache.aggregate.return_value = []
        return collector

    def make_shooter(self) -> SnapShooter:
        shooter = SnapShooter.__new__(SnapShooter)
        shooter.log = logging.getLogger(self.id())
        shooter.db = self.db
        config_dir = os.path.join(os.path.dirname(__file__), "..", "test_config")
        shooter.model_spec = ModelSpec(read_config_dir(config_dir, recursive=True)["db_entities"])
        shooter.snapshot_queue_writer = Mock()
        return shooter

    def test_ttl_collection(self):
        with self.assertRaises(JobFailure):
            self.make_collector().collect_ttl("ip")
        fields, _ = self.end_record("GarbageCollector", "ttl_collect_end")
        self.assertEqual(fields["error"], "JobFailure: boom")

    def test_weak_collection(self):
        with self.assertRaises(JobFailure):
            self.make_collector().collect_weak("ip")
        fields, _ = self.end_record("GarbageCollector", "weak_collect_end")
        self.assertEqual(fields["error"], "JobFailure: boom")

    def test_collection_counts_only_deleted_entities(self):
        self.db.get_worker_master_records = Mock(return_value=FailingCursor([{"_id": "a"}]))
        with self.assertRaises(JobFailure):
            self.make_collector().collect_weak("ip")
        self.db.delete_eids.assert_not_called()
        _, counts = self.end_record("GarbageCollector", "weak_collect_end")
        self.assertEqual(counts, {"entities": 1, "deleted": 0})

    def test_aggregation(self):
        manager = HistoryManager.__new__(HistoryManager)
        manager.log = logging.getLogger(self.id())
        manager.db = self.db
        manager.worker_index, manager.num_workers = 0, 1
        spec = SimpleNamespace(
            t=AttrType.OBSERVATIONS, history_params=SimpleNamespace(aggregate=True)
        )
        manager.model_spec = SimpleNamespace(
            entities={"ip": None}, entity_attributes={"ip": {"open_ports": spec}}
        )

        with self.assertRaises(JobFailure):
            manager.aggregate_master_docs()
        fields, _ = self.end_record("HistoryManager", "aggregation_end")
        self.assertEqual(fields["error"], "JobFailure: boom")
        self.assertEqual(fields["aggregation_metrics"]["entity_types"][0]["entity"], "ip")

    def test_snapshot_cleaning_continues_after_failed_entity_type(self):
        manager = HistoryManager.__new__(HistoryManager)
        manager.log = logging.getLogger(self.id())
        manager.db = self.db
        manager.keep_snapshot_delta = timedelta(days=30)
        manager.model_spec = SimpleNamespace(entities={"ip": None, "asn": None})
        self.db.snapshots = Mock()
        self.db.snapshots.delete_old.side_effect = [
            DatabaseError("boom"),
            {"buckets": 3, "oversized": 1},
        ]

        with self.assertLogs(manager.log, logging.ERROR):
            manager.delete_old_snapshots()

        (started,) = self.metadata.insert_one.call_args.args
        self.assertEqual(
            started["snapshot_cleaning_start"] - started["deleted_before"], timedelta(days=30)
        )
        fields, counts = self.end_record("HistoryManager", "snapshot_cleaning_end")
        self.assertNotIn("error", fields)
        self.assertEqual(counts, {"buckets": 3, "oversized": 1, "failures": 1})
        failed, cleaned = fields["snapshot_cleaning_metrics"]["entity_types"]
        self.assertEqual(failed["entity"], "ip")
        self.assertEqual(failed["error"], "DatabaseError: boom")
        self.assertEqual(
            {key: cleaned[key] for key in ("entity", "buckets", "oversized")},
            {"entity": "asn", "buckets": 3, "oversized": 1},
        )
        self.assertGreaterEqual(cleaned["duration"], 0)

    def test_snapshot_cleaning_runs_without_its_start_record(self):
        manager = HistoryManager.__new__(HistoryManager)
        manager.log = logging.getLogger(self.id())
        manager.db = self.db
        manager.keep_snapshot_delta = timedelta(days=30)
        manager.model_spec = SimpleNamespace(entities={"ip": None, "asn": None})
        self.db.snapshots = Mock()
        self.db.snapshots.delete_old.return_value = {"buckets": 1, "oversized": 0}
        self.metadata.insert_one.side_effect = RuntimeError("no primary")

        with self.assertLogs(METADATA_LOG, logging.ERROR):
            manager.delete_old_snapshots()

        self.assertEqual(
            [call.args[0] for call in self.db.snapshots.delete_old.call_args_list], ["ip", "asn"]
        )
        self.metadata.update_one.assert_not_called()

    def test_collection_runs_without_its_start_record(self):
        self.metadata.insert_one.side_effect = RuntimeError("no primary")
        for collect in (self.make_collector().collect_ttl, self.make_collector().collect_weak):
            self.db.get_worker_master_records = Mock(return_value=FailingCursor([]))
            with self.subTest(collect.__name__):
                with self.assertLogs(METADATA_LOG, logging.ERROR), self.assertRaises(JobFailure):
                    collect("ip")
                # The collection got to reading the master records
                self.db.get_worker_master_records.assert_called_once()
                self.metadata.update_one.assert_not_called()

    def test_snapshot_task_creation_runs_without_its_start_record(self):
        self.metadata.insert_one.side_effect = RuntimeError("no primary")
        shooter = self.make_shooter()
        with (
            patch.object(SnapShooter, "get_cached_link_entity_ids", return_value=[]),
            patch.object(SnapShooter, "get_linked_entities", return_value=[]),
            self.assertLogs(METADATA_LOG, logging.ERROR),
        ):
            shooter.make_snapshots()
        # Run start, linked entities and run end
        self.assertEqual(shooter.snapshot_queue_writer.broadcast_task.call_count, 3)
        self.metadata.update_one.assert_not_called()

    def test_snapshot_task_creation(self):
        with (
            patch.object(SnapShooter, "get_cached_link_entity_ids", side_effect=JobFailure("boom")),
            self.assertRaises(JobFailure),
        ):
            self.make_shooter().make_snapshots()
        fields, _ = self.end_record("SnapShooter", "task_creation_end")
        self.assertEqual(fields["error"], "JobFailure: boom")

    def test_snapshot_task_creation_succeeds(self):
        shooter = self.make_shooter()
        with (
            patch.object(
                SnapShooter, "get_cached_link_entity_ids", return_value=[("del_ttl", "e1")]
            ),
            patch.object(SnapShooter, "get_linked_entities", return_value=[[("del_ttl", "e1")]]),
        ):
            shooter.make_snapshots()
        self.assertEqual(shooter.snapshot_queue_writer.broadcast_task.call_count, 3)
        shooter.snapshot_queue_writer.put_task.assert_called_once()
        fields, counts = self.end_record("SnapShooter", "task_creation_end")
        self.assertNotIn("error", fields)
        self.assertEqual(counts, {"entities": 1, "components": 1})

    def test_snapshot_run_end_failure(self):
        shooter = self.make_shooter()
        writer = shooter.snapshot_queue_writer
        writer.broadcast_task.side_effect = [None, None, JobFailure("boom")]
        with (
            patch.object(SnapShooter, "get_cached_link_entity_ids", return_value=[]),
            patch.object(SnapShooter, "get_linked_entities", return_value=[]),
            self.assertRaises(JobFailure),
        ):
            shooter.make_snapshots()
        fields, _ = self.end_record("SnapShooter", "task_creation_end")
        self.assertEqual(fields["error"], "JobFailure: boom")

    def test_end_write_failure_keeps_original_error(self):
        self.metadata.update_one.side_effect = DatabaseError("metadata down")
        with (
            patch.object(SnapShooter, "get_cached_link_entity_ids", side_effect=JobFailure("boom")),
            self.assertLogs(self.db.metadata.log, logging.ERROR),
            self.assertRaises(JobFailure),
        ):
            self.make_shooter().make_snapshots()


if __name__ == "__main__":
    unittest.main()
