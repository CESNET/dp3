import logging
import unittest
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock

from dp3.snapshots.snapshooter import SnapShooter


class FakeCursor:
    def __init__(self, records):
        self.records = records
        self.closed = False

    def __iter__(self):
        if self.closed:
            return iter(())
        return iter(self.records)

    def close(self):
        self.closed = True


class FakeDatabase:
    def __init__(self, records):
        self.records = records
        self.cursors = []
        self.update_metadata = Mock()

    def get_worker_master_records(self, *args, **kwargs):
        cursor = FakeCursor(self.records)
        self.cursors.append(cursor)
        return cursor


def make_snapshooter(records):
    snapshooter = SnapShooter.__new__(SnapShooter)
    snapshooter.log = logging.getLogger("SnapShooterRetryTest")
    snapshooter.db = FakeDatabase(records)
    snapshooter.worker_index = 0
    snapshooter.worker_cnt = 1
    snapshooter.snapshot_entities = ["ip"]
    return snapshooter


class TestSnapShooterRetry(unittest.TestCase):
    def test_failure_closes_cursor_and_does_not_report_completion(self):
        records = [{"_id": "first"}, {"_id": "second"}]
        snapshooter = make_snapshooter(records)

        def process(_etype, records_cursor, _time, _have_links):
            next(iter(records_cursor))
            raise KeyError("writeErrors")

        snapshooter.make_linkless_snapshots = process
        task = SimpleNamespace(entities=[], time=datetime.now(UTC))

        snapshooter.make_snapshots_by_hash(task)

        self.assertEqual(len(snapshooter.db.cursors), 1)
        self.assertTrue(snapshooter.db.cursors[0].closed)
        snapshooter.db.update_metadata.assert_not_called()

    def test_retry_does_not_repeat_records_processed_before_failure(self):
        records = [{"_id": "first"}, {"_id": "second"}]
        snapshooter = make_snapshooter(records)
        attempts = 0
        processed = []

        def process(_etype, records_cursor, _time, _have_links):
            nonlocal attempts
            attempts += 1
            for record in records_cursor:
                processed.append(record["_id"])
                if attempts == 1:
                    raise RuntimeError("failed after processing the first record")
            return len(records)

        snapshooter.make_linkless_snapshots = process
        task = SimpleNamespace(entities=[], time=datetime.now(UTC))

        snapshooter.make_snapshots_by_hash(task)

        self.assertEqual(attempts, 1)
        self.assertEqual(len(processed), len(set(processed)), processed)
        self.assertTrue(snapshooter.db.cursors[0].closed)
        snapshooter.db.update_metadata.assert_not_called()

    def test_failure_does_not_report_worker_completion(self):
        snapshooter = make_snapshooter([{"_id": "first"}])
        snapshooter.make_linkless_snapshots = Mock(side_effect=RuntimeError("save failed"))
        task = SimpleNamespace(entities=[], time=datetime.now(UTC))

        snapshooter.make_snapshots_by_hash(task)

        self.assertEqual(snapshooter.make_linkless_snapshots.call_count, 1)
        self.assertEqual(len(snapshooter.db.cursors), 1)
        self.assertTrue(all(cursor.closed for cursor in snapshooter.db.cursors))
        snapshooter.db.update_metadata.assert_not_called()


if __name__ == "__main__":
    unittest.main()
