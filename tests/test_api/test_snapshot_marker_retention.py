import unittest
from datetime import UTC, datetime, timedelta

import common

from dp3.database.config import MongoConfig
from dp3.database.database import EntityDatabase
from dp3.database.snapshots import SnapshotCollectionContainer


class OversizedSnapshotMarkerRetention(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        db_config = MongoConfig.model_validate(common.CONFIG.get("database", {}))
        cls.client = EntityDatabase.connect(db_config)
        cls.client.admin.command("ping")
        cls.snapshots = SnapshotCollectionContainer(
            cls.client[db_config.db_name],
            db_config,
            common.MODEL_SPEC,
            common.CONFIG.get("snapshots", {}),
        )["B"]

    @classmethod
    def tearDownClass(cls):
        cls.client.close()

    def setUp(self):
        self.eid = "oversized-marker-retention-regression"
        self.snapshot_col = self.snapshots._col()
        self.oversized_col = self.snapshots._os_col()
        self._clear_test_data()

    def tearDown(self):
        self._clear_test_data()

    def _clear_test_data(self):
        self.snapshot_col.delete_many(self.snapshots._filter_from_eid(self.eid))
        self.oversized_col.delete_many({"eid": self.eid})
        self.snapshots._normal_snapshot_eids.discard(self.eid)
        self.snapshots._oversized_snapshot_eids.discard(self.eid)

    def test_marker_time_tracks_latest_oversized_snapshot(self):
        now = datetime.now(UTC).replace(microsecond=0)
        old_time = now - timedelta(days=30)
        migration_time = now - timedelta(hours=2)
        save_one_time = now - timedelta(hours=1)
        save_many_time = now

        old_snapshot = {
            "eid": self.eid,
            "_time_created": old_time,
            "data1": "old",
        }
        self.snapshot_col.insert_one(
            {
                "_id": self.snapshots._bucket_id(self.eid, old_time),
                "_time_created": old_time,
                "oversized": False,
                "latest": True,
                "count": 1,
                "last": old_snapshot,
                "history": [old_snapshot],
            }
        )

        migrated_snapshot = {
            "eid": self.eid,
            "_time_created": migration_time,
            "data1": "migrated",
        }
        self.snapshots._migrate_to_oversized(self.eid, migrated_snapshot)
        marker = self.snapshot_col.find_one(self.snapshots._filter_from_eid(self.eid))
        self.assertEqual(marker["_time_created"], migration_time)

        self.snapshots.save_one({"eid": self.eid, "data1": "save-one"}, save_one_time)
        marker = self.snapshot_col.find_one(self.snapshots._filter_from_eid(self.eid))
        self.assertEqual(marker["_time_created"], save_one_time)

        self.snapshots.save_many([{"eid": self.eid, "data1": "save-many"}], save_many_time)
        marker = self.snapshot_col.find_one(self.snapshots._filter_from_eid(self.eid))
        self.assertEqual(marker["_time_created"], save_many_time)

        retention_cutoff = now - timedelta(days=7)
        self.snapshots.delete_old(retention_cutoff)

        marker = self.snapshot_col.find_one(self.snapshots._filter_from_eid(self.eid))
        self.assertIsNotNone(marker)
        self.assertEqual(marker["last"]["data1"], "save-many")
        self.assertEqual(self.oversized_col.count_documents({"eid": self.eid}), 3)
        self.assertEqual(len(list(self.snapshots.get_by_eid(self.eid))), 3)


if __name__ == "__main__":
    unittest.main()
