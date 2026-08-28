import unittest
from datetime import UTC, datetime

import common

from dp3.database.config import MongoConfig
from dp3.database.database import EntityDatabase
from dp3.database.snapshots import SnapshotCollectionContainer


class StringEidSnapshotRegexLimit(unittest.TestCase):
    """Exercise batched deletion of snapshot buckets with long string EIDs."""

    @classmethod
    def setUpClass(cls):
        db_config = MongoConfig.model_validate(common.CONFIG.get("database", {}))
        cls.client = EntityDatabase.connect(db_config)
        cls.snapshots = SnapshotCollectionContainer(
            cls.client[db_config.db_name],
            db_config,
            common.MODEL_SPEC,
            common.CONFIG.get("snapshots", {}),
        )["del_ttl"]

    @classmethod
    def tearDownClass(cls):
        cls.client.close()

    def setUp(self):
        prefix = "snapshot-regex-limit-"
        self.eids = [f"https://example.test/{prefix}{index:04d}/{'x' * 96}" for index in range(400)]
        created_at = datetime.now(UTC).replace(microsecond=0)
        self.bucket_ids = [self.snapshots._bucket_id(eid, created_at) for eid in self.eids]
        self.snapshots._col().insert_many(
            [
                {
                    "_id": bucket_id,
                    "history": [],
                    "last": {"eid": eid, "_time_created": created_at},
                    "_time_created": created_at,
                    "count": 0,
                    "oversized": False,
                    "latest": True,
                }
                for eid, bucket_id in zip(self.eids, self.bucket_ids, strict=True)
            ]
        )

    def tearDown(self):
        self.snapshots._col().delete_many({"_id": {"$in": self.bucket_ids}})
        self.snapshots._os_col().delete_many({"eid": {"$in": self.eids}})
        self.snapshots._invalidate_snapshot_state(self.eids)

    def test_batched_delete_supports_long_string_eids(self):
        deleted = self.snapshots.delete_eids(self.eids)

        self.assertEqual(deleted, len(self.eids) * self.snapshots._snapshot_bucket_size)
        self.assertEqual(
            self.snapshots._col().count_documents({"_id": {"$in": self.bucket_ids}}), 0
        )


if __name__ == "__main__":
    unittest.main()
