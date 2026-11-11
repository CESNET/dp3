import gzip
import json
import logging
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from pymongo.errors import AutoReconnect, OperationFailure

from dp3.database import database
from dp3.database.database import (
    DatabaseError,
    EntityDatabase,
    is_transient_db_error,
    run_with_db_retries,
)
from dp3.history_management import history_manager
from dp3.history_management.history_manager import HistoryManager

# Archivation runs in real time; the test data is placed relative to it
NOW = datetime.now(UTC).replace(microsecond=0)


def archive_name(etype: str, created: datetime) -> str:
    return f"{etype}#archive_{created.strftime('%Y_%m_%d_%H%M%S')}"


def dp(etype: str, eid: str, t1: datetime | None, v=1) -> dict:
    return {"_id": f"{eid}-{t1}-{v}", "etype": etype, "eid": eid, "attr": "a", "v": v, "t1": t1}


class FakeCursor:
    def __init__(self, docs, fail_after=None, error=None):
        self.docs = docs
        self.fail_after = fail_after
        self.error = error
        self.closed = False

    def __iter__(self):
        for i, doc in enumerate(self.docs):
            if self.fail_after is not None and i == self.fail_after:
                raise self.error
            yield dict(doc)

    def close(self):
        self.closed = True


class FakeMetadata:
    def __init__(self):
        self.records = {}

    def save(self, time, metadata):
        self.records[time] = dict(metadata)

    def update(self, time, metadata):
        self.records.setdefault(time, {}).update(metadata)


class FakeDatabase:
    """In-memory stand-in for the archive related part of `EntityDatabase`."""

    def __init__(self):
        self.raw = {}
        self.archives = {}
        self.metadata = FakeMetadata()
        self.export_failures = []  # (archive name, fail after n docs, error), consumed in order
        self.rename_errors = {}
        self.cursors = []
        self.created = {}  # entity type -> archive created by the run

    get_archive_col_time = staticmethod(EntityDatabase.get_archive_col_time)

    def move_raw_to_archive(self, etype):
        if etype in self.rename_errors:
            raise self.rename_errors[etype]
        if not self.raw.get(etype):
            return None
        name = archive_name(etype, datetime.now(UTC))
        self.archives[name] = self.raw.pop(etype)
        self.created[etype] = name
        return name

    def get_archive_col_names(self, etype):
        return sorted(name for name in self.archives if name.startswith(f"{etype}#archive_"))

    def get_archived_dps(self, archive_col_name, batch_size=0):
        cursor = FakeCursor(self.archives[archive_col_name])
        if self.export_failures and self.export_failures[0][0] == archive_col_name:
            _, cursor.fail_after, cursor.error = self.export_failures.pop(0)
        self.cursors.append(cursor)
        return cursor

    def drop_archive(self, archive_col_name):
        del self.archives[archive_col_name]
        return True

    def drop_empty_archives(self, etype):
        empty = [name for name in self.get_archive_col_names(etype) if not self.archives[name]]
        for name in empty:
            del self.archives[name]
        return len(empty)


class TestArchivation(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.archive_dir = Path(self.tmp.name)
        self.db = FakeDatabase()

        self.manager = HistoryManager.__new__(HistoryManager)
        self.manager.log = logging.getLogger("ArchivationTest")
        self.manager.db = self.db
        self.manager.model_spec = SimpleNamespace(entities={"ip": None, "mac": None})
        self.manager.keep_raw_delta = timedelta(days=1)
        self.manager.log_dir = self.archive_dir

        retry_patch = patch.object(database, "DB_RETRY_DELAYS", (0, 0))
        retry_patch.start()
        self.addCleanup(retry_patch.stop)

        self.yesterday = archive_name("ip", NOW - timedelta(days=1))
        self.old = dp("ip", "1.1.1.1", NOW - timedelta(days=1, hours=5))
        self.plain = dp("ip", "1.1.1.2", None)
        self.future = dp("ip", "1.1.1.3", NOW + timedelta(hours=2))
        self.db.archives[self.yesterday] = [self.old, self.plain, self.future]
        self.today = dp("ip", "1.1.1.4", NOW - timedelta(minutes=5))
        self.db.raw["ip"] = [self.today]

    def tearDown(self):
        self.tmp.cleanup()

    def exported(self) -> list[dict]:
        files = list(self.archive_dir.iterdir())
        self.assertEqual(len(files), 1, files)
        self.assertTrue(files[0].name.endswith(".jsonl.gz"), files[0])
        with gzip.open(files[0], "rt", encoding="utf-8") as f:
            return [json.loads(line) for line in f]

    def metrics(self) -> dict:
        (metadata,) = self.db.metadata.records.values()
        return metadata["archivation_metrics"]

    @staticmethod
    def export_name(t_min: datetime, t_max: datetime, suffix: str = "") -> str:
        fmt = "%Y%m%dT%H%M%S"
        return f"dp-log-{t_min.strftime(fmt)}--{t_max.strftime(fmt)}{suffix}.jsonl.gz"

    def test_exports_whole_archive_and_drops_it(self):
        self.manager.archive_old_dps()

        self.assertEqual({dp["id"] for dp in self.exported()}, {"1.1.1.1", "1.1.1.2", "1.1.1.3"})
        self.assertNotIn(self.yesterday, self.db.archives)
        # The archive created by this run is left for the next run, nothing goes back to raw
        self.assertEqual(self.db.archives[self.db.created["ip"]], [self.today])
        self.assertNotIn("ip", self.db.raw)
        self.assertEqual(
            self.metrics() | {"file": None},
            {
                "archives": 1,
                "exported": 3,
                "future_dated": 1,
                "dropped": 1,
                "export_failed": False,
                "failures": 0,
                "file": None,
            },
        )
        # The range in the file name covers all exported t1 values, including future-dated ones
        expected = self.export_name(self.old["t1"], self.future["t1"])
        self.assertEqual(Path(self.metrics()["file"]).name, expected)

    def test_archive_with_only_future_dated_dps(self):
        later = dp("ip", "1.1.1.5", NOW + timedelta(days=3))
        self.db.archives[self.yesterday] = [self.future, later]

        with self.assertLogs("ArchivationTest", level="WARNING") as logs:
            self.manager.archive_old_dps()

        self.assertEqual(len(self.exported()), 2)
        expected = self.export_name(self.future["t1"], later["t1"])
        self.assertEqual(Path(self.metrics()["file"]).name, expected)
        self.assertEqual(self.metrics()["future_dated"], 2)
        self.assertIn("later than the creation of the archive collection", logs.output[0])

    def test_archive_without_t1_is_named_by_run_start(self):
        self.db.archives[self.yesterday] = [self.plain]
        before = datetime.now(UTC).replace(microsecond=0)

        self.manager.archive_old_dps()

        self.assertEqual(len(self.exported()), 1)
        name = Path(self.metrics()["file"]).name
        names = {
            self.export_name(t, t)
            for t in (before, before + timedelta(seconds=1), before + timedelta(seconds=2))
        }
        self.assertIn(name, names)

    def test_archive_created_shortly_after_cutoff_is_processed(self):
        within = archive_name("ip", NOW - timedelta(days=1) + timedelta(minutes=30))
        too_new = archive_name("ip", NOW - timedelta(hours=12))
        self.db.archives[within] = [self.old]
        self.db.archives[too_new] = [self.today]

        self.manager.archive_old_dps()

        self.assertNotIn(within, self.db.archives)
        self.assertEqual(self.db.archives[too_new], [self.today])

    def test_zero_retention_keeps_archive_created_by_this_run(self):
        self.manager.keep_raw_delta = timedelta(0)

        self.manager.archive_old_dps()

        self.assertNotIn(self.yesterday, self.db.archives)
        self.assertEqual(self.db.archives[self.db.created["ip"]], [self.today])

    def test_failed_compression_leaves_no_files(self):
        def failing_compress(original, compressed):
            compressed.write_bytes(b"incomplete")
            raise OSError("No space left on device")

        with patch.object(history_manager, "compress_file", failing_compress):
            self.manager.archive_old_dps()

        self.assertEqual(list(self.archive_dir.iterdir()), [])
        self.assertIn(self.yesterday, self.db.archives)
        self.assertTrue(self.metrics()["export_failed"])

    def test_transient_error_is_retried_without_duplicates(self):
        self.db.export_failures = [(self.yesterday, 1, AutoReconnect("connection reset"))]

        self.manager.archive_old_dps()

        ids = sorted(dp["id"] for dp in self.exported())
        self.assertEqual(ids, ["1.1.1.1", "1.1.1.2", "1.1.1.3"])
        self.assertNotIn(self.yesterday, self.db.archives)
        self.assertTrue(all(cursor.closed for cursor in self.db.cursors))

    def test_transient_error_wrapped_in_database_error_is_retried(self):
        error = DatabaseError("fetch failed")
        error.__cause__ = OperationFailure("cursor not found", code=43)
        self.db.export_failures = [(self.yesterday, 0, error)]

        self.manager.archive_old_dps()

        self.assertEqual(len(self.exported()), 3)

    def test_failed_export_keeps_archives_and_removes_partial_file(self):
        empty = archive_name("mac", NOW - timedelta(days=3))
        self.db.archives[empty] = []
        self.db.export_failures = [(self.yesterday, 1, ValueError("not serializable"))]

        self.manager.archive_old_dps()

        self.assertEqual(list(self.archive_dir.iterdir()), [])
        self.assertEqual(self.db.archives[self.yesterday], [self.old, self.plain, self.future])
        # Empty archives are dropped even if the export fails
        self.assertNotIn(empty, self.db.archives)
        self.assertTrue(self.metrics()["export_failed"])

    def test_exhausted_retries_keep_archives(self):
        error = AutoReconnect("connection reset")
        self.db.export_failures = [(self.yesterday, 0, error)] * 3

        self.manager.archive_old_dps()

        self.assertEqual(list(self.archive_dir.iterdir()), [])
        self.assertIn(self.yesterday, self.db.archives)

    def test_failure_of_one_entity_type_does_not_stop_others(self):
        self.db.rename_errors["ip"] = DatabaseError("rename failed")
        mac_archive = archive_name("mac", NOW - timedelta(days=2))
        self.db.archives[mac_archive] = [dp("mac", "de:ad:be:ef:00:01", NOW - timedelta(days=2))]

        self.manager.archive_old_dps()

        self.assertEqual(len(self.exported()), 4)
        self.assertNotIn(self.yesterday, self.db.archives)
        self.assertNotIn(mac_archive, self.db.archives)
        self.assertEqual(self.metrics()["failures"], 1)

    def test_without_archive_dir_archives_are_dropped_without_export(self):
        self.manager.log_dir = None

        self.manager.archive_old_dps()

        self.assertNotIn(self.yesterday, self.db.archives)
        self.assertIn(self.db.created["ip"], self.db.archives)

    def test_existing_export_file_is_not_overwritten(self):
        name = self.export_name(self.old["t1"], self.future["t1"])
        (self.archive_dir / name).write_bytes(b"previous")

        self.manager.archive_old_dps()

        self.assertEqual((self.archive_dir / name).read_bytes(), b"previous")
        suffixed = self.export_name(self.old["t1"], self.future["t1"], "-1")
        self.assertTrue((self.archive_dir / suffixed).exists())


class TestArchiveHelpers(unittest.TestCase):
    def test_archive_col_time(self):
        name = archive_name("ip", NOW)
        self.assertEqual(EntityDatabase.get_archive_col_time("ip", name), NOW)
        self.assertIsNone(EntityDatabase.get_archive_col_time("ip", "ip#archive_unknown"))
        self.assertIsNone(EntityDatabase.get_archive_col_time("ip", archive_name("vip", NOW)))

    def test_is_transient_db_error(self):
        self.assertTrue(is_transient_db_error(AutoReconnect("reset")))
        wrapped = DatabaseError("failed")
        wrapped.__cause__ = OperationFailure("stepped down", code=11602)
        self.assertTrue(is_transient_db_error(wrapped))
        self.assertFalse(is_transient_db_error(OperationFailure("bad query", code=2)))
        self.assertFalse(is_transient_db_error(ValueError("bad value")))

    def test_run_with_db_retries(self):
        log = logging.getLogger("ArchivationTest")
        attempts = []

        def flaky():
            attempts.append(1)
            if len(attempts) < 3:
                raise AutoReconnect("reset")
            return "done"

        self.assertEqual(run_with_db_retries(flaky, "Flaky", log, delays=(0, 0)), "done")
        self.assertEqual(len(attempts), 3)

        def failing():
            attempts.append(1)
            raise ValueError("bad value")

        attempts.clear()
        with self.assertRaises(ValueError):
            run_with_db_retries(failing, "Failing", log, delays=(0, 0))
        self.assertEqual(len(attempts), 1)


if __name__ == "__main__":
    unittest.main()
