import unittest
from datetime import UTC, datetime
from unittest.mock import Mock

from dp3.database.metadata import MetadataCollection


class Recorder:
    """A module saving its records, as e.g. `GarbageCollector` does."""

    def __init__(self, metadata: MetadataCollection):
        self.metadata = metadata
        self.start = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)

    def run(self):
        self.metadata.save(self.start, {"run_start": self.start})
        self.metadata.update(self.start, {"progress": 1})
        with self.metadata.record_run_end(self.start, "run_end"):
            pass


class MetadataRecordModule(unittest.TestCase):
    def test_records_belong_to_the_calling_class(self):
        # The `#module` is read from the call stack, an extra call between
        # the module and the collection would silently rename all records.
        collection = Mock()
        collection.update_one.return_value = Mock(matched_count=1)

        Recorder(MetadataCollection({"#metadata": collection}, 2)).run()

        record = collection.insert_one.call_args.args[0]
        self.assertEqual(record["#module"], "Recorder")
        self.assertEqual(record["_id"], "Recorder2026-01-02T03:04:05.000000Zw2")
        updated_ids = [c.args[0]["_id"] for c in collection.update_one.call_args_list]
        self.assertEqual(updated_ids, [record["_id"], record["_id"]])


if __name__ == "__main__":
    unittest.main()
