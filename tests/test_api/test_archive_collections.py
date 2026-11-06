import unittest
from datetime import UTC, datetime, timedelta

import common

from dp3.database.config import MongoConfig
from dp3.database.database import EntityDatabase
from dp3.database.encodings import get_codec_options

# Not a configured entity type, so the tests never touch collections used by the running workers
ETYPE = "archive_collections_test"


class ArchiveCollections(unittest.TestCase):
    """Archive collection operations of `EntityDatabase`, only MongoDB is required."""

    @classmethod
    def setUpClass(cls):
        db_config = MongoConfig.model_validate(common.CONFIG.get("database", {}))
        cls.client = EntityDatabase.connect(db_config)
        cls.client.admin.command("ping")
        cls.db = cls.client.get_database(db_config.db_name, codec_options=get_codec_options())

        cls.entity_db = EntityDatabase.bare(common.MODEL_SPEC, cls.db)

    @classmethod
    def tearDownClass(cls):
        cls.client.close()

    def setUp(self):
        self.now = datetime.now(UTC).replace(microsecond=0)
        self.cutoff = self.now - timedelta(days=1)
        self.archive = f"{ETYPE}#archive_2000_01_01_000000"
        self.other_archive = f"{ETYPE}#archive_2000_01_02_000000"
        self.similar_etype_archive = f"x{ETYPE}#archive_2000_01_01_000000"
        self.raw = f"{ETYPE}#raw"
        self._clear()

    def tearDown(self):
        self._clear()

    def _clear(self):
        for name in (self.archive, self.other_archive, self.similar_etype_archive, self.raw):
            self.db.drop_collection(name)

    def test_archive_names_are_anchored_and_sorted(self):
        for name in (self.other_archive, self.archive, self.similar_etype_archive):
            self.db[name].insert_one({"_id": 1})

        self.assertEqual(
            self.entity_db.get_archive_col_names(ETYPE), [self.archive, self.other_archive]
        )
        self.assertEqual(
            self.entity_db.get_archive_col_time(ETYPE, self.archive),
            datetime(2000, 1, 1, tzinfo=UTC),
        )

    def test_whole_archive_is_exported_and_dropped(self):
        dps = [
            {"_id": "old", "t1": self.cutoff - timedelta(hours=1)},
            {"_id": "plain"},
            {"_id": "future", "t1": self.now + timedelta(days=1)},
            {"_id": "malformed", "t1": "2000-01-01T00:00:00"},
        ]
        self.db[self.archive].insert_many(dps)

        exported = {
            doc["_id"] for doc in self.entity_db.get_archived_dps(self.archive, batch_size=2)
        }
        self.assertEqual(exported, {"old", "plain", "future", "malformed"})

        self.entity_db.drop_archive(self.archive)
        self.assertNotIn(self.archive, self.db.list_collection_names())

    def test_drop_empty_archives(self):
        self.db.create_collection(self.archive)
        self.db[self.other_archive].insert_one({"_id": 1})

        self.assertEqual(self.entity_db.drop_empty_archives(ETYPE), 1)
        self.assertEqual(self.entity_db.get_archive_col_names(ETYPE), [self.other_archive])

    def test_move_raw_to_archive(self):
        self.assertIsNone(self.entity_db.move_raw_to_archive(f"missing_{ETYPE}"))

        self.db[self.raw].insert_one({"_id": 1})
        name = self.entity_db.move_raw_to_archive(ETYPE)
        self.addCleanup(self.db.drop_collection, name)

        self.assertIsNotNone(self.entity_db.get_archive_col_time(ETYPE, name))
        self.assertEqual(self.db[name].count_documents({}), 1)
        self.assertNotIn(self.raw, self.db.list_collection_names())


if __name__ == "__main__":
    unittest.main()
