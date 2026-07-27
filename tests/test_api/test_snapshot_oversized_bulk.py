import unittest
from datetime import UTC, datetime
from ipaddress import IPv4Address
from unittest.mock import patch

import common
from bson import BSON
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError, OperationFailure

from dp3.database import snapshots as snapshots_module
from dp3.database.config import MongoConfig
from dp3.database.database import EntityDatabase
from dp3.database.encodings import BSON_OBJECT_TOO_LARGE
from dp3.database.snapshots import SnapshotCollectionContainer


class OversizedBulkSnapshot(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        db_config = MongoConfig.model_validate(common.CONFIG.get("database", {}))
        cls.client = EntityDatabase.connect(db_config)
        cls.max_bson_size = cls.client.admin.command("hello")["maxBsonObjectSize"]
        cls.snapshots = SnapshotCollectionContainer(
            cls.client[db_config.db_name],
            db_config,
            common.MODEL_SPEC,
            common.CONFIG.get("snapshots", {}),
        )["del_ttl_ip"]

    @classmethod
    def tearDownClass(cls):
        cls.client.close()

    def setUp(self):
        self.eid = IPv4Address("198.51.100.242")
        self.other_eids = (
            IPv4Address("198.51.100.243"),
            IPv4Address("198.51.100.244"),
        )
        self._clear_test_eids()

    def tearDown(self):
        self._clear_test_eids()

    def _clear_test_eids(self):
        eids = {self.eid, *self.other_eids}
        for eid in eids:
            self.snapshots._col().delete_many(self.snapshots._filter_from_eid(eid))
        self.snapshots._os_col().delete_many({"eid": {"$in": list(eids)}})
        self.snapshots._normal_snapshot_eids.difference_update(eids)
        self.snapshots._oversized_snapshot_eids.difference_update(eids)

    def _fail_first_normal_bulk_write(self, error):
        collection_type = type(self.snapshots._col())
        original_bulk_write = collection_type.bulk_write
        normal_collection_name = self.snapshots._col_name
        failed = False

        def bulk_write(collection, *args, **kwargs):
            nonlocal failed
            if collection.name == normal_collection_name and not failed:
                failed = True
                raise error
            return original_bulk_write(collection, *args, **kwargs)

        return patch.object(collection_type, "bulk_write", new=bulk_write)

    def test_large_snapshot_whose_bulk_update_exceeds_bson_limit_uses_oversized_storage(self):
        created_at = datetime.now(UTC).replace(microsecond=0)
        snapshot = {
            "eid": self.eid,
            "payload": "x" * 13_800_000,
            "_time_created": created_at,
        }
        query = self.snapshots._filter_from_eid(self.eid) | {
            "count": {"$lt": self.snapshots._snapshot_bucket_size}
        }
        update = {
            "$set": {"last": snapshot},
            "$push": {"history": {"$each": [snapshot], "$position": 0}},
            "$inc": {"count": 1},
            "$setOnInsert": {
                "_id": self.snapshots._bucket_id(self.eid, created_at),
                "_time_created": created_at,
                "oversized": False,
                "latest": True,
            },
        }
        codec_options = self.snapshots._db.codec_options
        snapshot_size = len(BSON.encode(snapshot, codec_options=codec_options))
        update_size = len(
            BSON.encode(
                {"q": query, "u": update, "multi": False, "upsert": True},
                codec_options=codec_options,
            )
        )

        self.assertLess(snapshot_size, self.max_bson_size)
        self.assertGreater(update_size, self.max_bson_size)

        self.snapshots.save_many([snapshot], created_at)

        marker = self.snapshots._col().find_one(self.snapshots._filter_from_eid(self.eid))
        self.assertIsNotNone(marker)
        self.assertTrue(marker["oversized"])
        self.assertEqual(marker["last"]["eid"], self.eid)
        self.assertEqual(marker["last"]["_time_created"], created_at)
        self.assertNotIn("history", marker)

        stored = list(self.snapshots._os_col().find({"eid": self.eid}, {"_id": False}))
        self.assertEqual(len(stored), 1)
        self.assertEqual(stored[0]["eid"], self.eid)
        self.assertEqual(stored[0]["_time_created"], created_at)
        self.assertEqual(len(stored[0]["payload"]), 13_800_000)

        restarted = SnapshotCollectionContainer(
            self.snapshots._db,
            MongoConfig.model_validate(common.CONFIG.get("database", {})),
            common.MODEL_SPEC,
            common.CONFIG.get("snapshots", {}),
        )["del_ttl_ip"]
        self.assertEqual(restarted._get_state({self.eid}), (set(), {self.eid}))

    def test_command_level_bson_error_recursively_splits_bulk(self):
        created_at = datetime.now(UTC).replace(microsecond=0)
        eids = [self.eid, *self.other_eids]
        snapshots = [{"eid": eid, "payload": str(eid)} for eid in eids]
        collection_type = type(self.snapshots._col())
        original_bulk_write = collection_type.bulk_write
        normal_collection_name = self.snapshots._col_name
        attempted_batch_sizes = []

        def bulk_write(collection, requests, *args, **kwargs):
            if collection.name == normal_collection_name and all(
                isinstance(request, UpdateOne) for request in requests
            ):
                attempted_batch_sizes.append(len(requests))
                if len(requests) > 1:
                    raise OperationFailure(
                        "BSON size limit hit while building Message",
                        code=BSON_OBJECT_TOO_LARGE,
                        details={"ok": 0.0, "code": BSON_OBJECT_TOO_LARGE},
                    )
            return original_bulk_write(collection, requests, *args, **kwargs)

        with patch.object(collection_type, "bulk_write", new=bulk_write):
            self.snapshots.save_many(snapshots, created_at)

        self.assertEqual(attempted_batch_sizes, [3, 1, 2, 1, 1])
        for eid in eids:
            marker = self.snapshots._col().find_one(self.snapshots._filter_from_eid(eid))
            self.assertIsNotNone(marker)
            self.assertEqual(marker["last"]["eid"], eid)

    def test_bulk_write_error_migrates_only_failed_update(self):
        created_at = datetime.now(UTC).replace(microsecond=0)
        eids = [self.eid, *self.other_eids]
        snapshots = [{"eid": eid, "payload": str(eid)} for eid in eids]
        collection_type = type(self.snapshots._col())
        original_bulk_write = collection_type.bulk_write
        normal_collection_name = self.snapshots._col_name
        failed = False

        def bulk_write(collection, requests, *args, **kwargs):
            nonlocal failed
            if (
                collection.name == normal_collection_name
                and not failed
                and all(isinstance(request, UpdateOne) for request in requests)
            ):
                failed = True
                successful_result = original_bulk_write(
                    collection, [requests[0], requests[2]], *args, **kwargs
                )
                upserted_ids = list(successful_result.upserted_ids.values())
                raise BulkWriteError(
                    {
                        "writeErrors": [{"index": 1, "code": BSON_OBJECT_TOO_LARGE}],
                        "writeConcernErrors": [],
                        "nInserted": 0,
                        "nUpserted": 2,
                        "nMatched": 0,
                        "nModified": 0,
                        "nRemoved": 0,
                        "upserted": [
                            {"index": 0, "_id": upserted_ids[0]},
                            {"index": 2, "_id": upserted_ids[1]},
                        ],
                    }
                )
            return original_bulk_write(collection, requests, *args, **kwargs)

        with patch.object(collection_type, "bulk_write", new=bulk_write):
            self.snapshots.save_many(snapshots, created_at)

        states = [self.snapshots._get_state({eid}) for eid in eids]
        self.assertEqual(sum(bool(oversized) for _, oversized in states), 1)
        for eid in eids:
            marker = self.snapshots._col().find_one(self.snapshots._filter_from_eid(eid))
            self.assertIsNotNone(marker)
            self.assertEqual(marker["last"]["eid"], eid)

    def test_single_update_command_level_bson_error_uses_oversized_storage(self):
        created_at = datetime.now(UTC).replace(microsecond=0)
        snapshot = {"eid": self.eid, "payload": "small"}
        error = OperationFailure(
            "BSON size limit hit while building Message",
            code=BSON_OBJECT_TOO_LARGE,
            details={"ok": 0.0, "code": BSON_OBJECT_TOO_LARGE},
        )

        with self._fail_first_normal_bulk_write(error):
            self.snapshots.save_many([snapshot], created_at)

        marker = self.snapshots._col().find_one(self.snapshots._filter_from_eid(self.eid))
        self.assertTrue(marker["oversized"])
        self.assertEqual(marker["last"]["eid"], self.eid)
        self.assertEqual(self.snapshots._os_col().count_documents({"eid": self.eid}), 1)

    def test_preflight_migration_updates_cache_before_later_bulk_failure(self):
        created_at = datetime.now(UTC).replace(microsecond=0)
        normal_eid = next(iter(self.other_eids))
        snapshots = [
            {"eid": self.eid, "payload": "x" * 20_000},
            {"eid": normal_eid, "payload": "small"},
        ]
        error = OperationFailure(
            "injected failure after oversized migration",
            code=91,
            details={"ok": 0.0, "code": 91},
        )

        with (
            patch.object(snapshots_module, "BSON_MAX_SIZE", 10_000),
            self._fail_first_normal_bulk_write(error),
            self.assertRaises(OperationFailure),
        ):
            self.snapshots.save_many(snapshots, created_at)

        marker = self.snapshots._col().find_one(self.snapshots._filter_from_eid(self.eid))
        self.assertIsNotNone(marker)
        self.assertTrue(marker["oversized"])
        self.assertEqual(self.snapshots._get_state({self.eid}), (set(), {self.eid}))

    def test_preflight_migration_keeps_last_input_snapshot_as_marker(self):
        created_at = datetime.now(UTC).replace(microsecond=0)
        snapshots = [
            {"eid": self.eid, "sequence": "first"},
            {"eid": self.eid, "sequence": "last"},
        ]

        with patch.object(snapshots_module, "BSON_MAX_SIZE", 0):
            self.snapshots.save_many(snapshots, created_at)

        marker = self.snapshots._col().find_one(self.snapshots._filter_from_eid(self.eid))
        self.assertEqual(marker["last"]["sequence"], "last")
        self.assertEqual(self.snapshots._os_col().count_documents({"eid": self.eid}), 2)


if __name__ == "__main__":
    unittest.main()
