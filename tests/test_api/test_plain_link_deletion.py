import unittest
from datetime import UTC, datetime
from types import SimpleNamespace

import common

from dp3.common.attrspec import AttrType
from dp3.core.link_manager import LinkManager
from dp3.database.config import MongoConfig
from dp3.database.database import EntityDatabase
from dp3.database.encodings import get_codec_options


class PlainLinkDeletion(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        db_config = MongoConfig.model_validate(common.CONFIG.get("database", {}))
        cls.client = EntityDatabase.connect(db_config)
        cls.client.admin.command("ping")
        cls.db = cls.client.get_database(db_config.db_name, codec_options=get_codec_options())
        cls.master = cls.db["plain_link_regression#master"]
        cls.cache = cls.db["#cache#PlainLinkRegression"]

        attr_specs = {
            "scalar_link": SimpleNamespace(t=AttrType.PLAIN, is_iterable=False),
            "iterable_link": SimpleNamespace(t=AttrType.PLAIN, is_iterable=True),
        }
        cls.entity_db = EntityDatabase.__new__(EntityDatabase)
        cls.entity_db._db_schema_config = SimpleNamespace(
            attr=lambda _etype, attr: attr_specs[attr]
        )
        cls.entity_db._master_col = lambda _etype: cls.master

        cls.link_manager = LinkManager.__new__(LinkManager)
        cls.link_manager.cache = cls.cache
        cls.link_manager.max_date = datetime.max.replace(tzinfo=UTC)

    @classmethod
    def tearDownClass(cls):
        cls.master.drop()
        cls.cache.drop()
        cls.client.close()

    def setUp(self):
        self.master.delete_many({})
        self.cache.delete_many({})

    @staticmethod
    def _datapoint(attr, value):
        return SimpleNamespace(etype="source", attr=attr, v=value)

    def test_scalar_cache_rewrite_replaces_old_target(self):
        self.link_manager.add_plain_to_link_cache(
            "target", "source-1", self._datapoint("scalar_link", SimpleNamespace(eid="old"))
        )
        self.link_manager.add_plain_to_link_cache(
            "target", "source-1", self._datapoint("scalar_link", SimpleNamespace(eid="new"))
        )

        links = list(self.cache.find({}, {"_id": 0, "to": 1}))
        self.assertEqual(links, [{"to": "target#new"}])

    def test_deleting_stale_scalar_target_preserves_current_link(self):
        self.master.insert_one(
            {
                "_id": "source-1",
                "scalar_link": {
                    "v": {"eid": "new"},
                    "ts_last_update": datetime.now(UTC),
                },
            }
        )

        self.entity_db.delete_link_dps("source", ["source-1"], "scalar_link", "old")

        document = self.master.find_one({"_id": "source-1"})
        self.assertEqual(document["scalar_link"]["v"]["eid"], "new")
        self.assertNotIn("#revision", document)

    def test_iterable_cache_rewrite_replaces_target_set(self):
        self.link_manager.add_iterable_plain_to_link_cache(
            "target",
            "source-1",
            self._datapoint(
                "iterable_link", [SimpleNamespace(eid="old"), SimpleNamespace(eid="kept")]
            ),
        )
        self.link_manager.add_iterable_plain_to_link_cache(
            "target",
            "source-1",
            self._datapoint(
                "iterable_link", [SimpleNamespace(eid="kept"), SimpleNamespace(eid="new")]
            ),
        )

        links = sorted(doc["to"] for doc in self.cache.find({}, {"_id": 0, "to": 1}))
        self.assertEqual(links, ["target#kept", "target#new"])

    def test_deleting_iterable_target_preserves_other_targets(self):
        self.master.insert_one(
            {
                "_id": "source-1",
                "iterable_link": {
                    "v": [{"eid": "deleted"}, {"eid": "kept"}],
                    "ts_last_update": datetime.now(UTC),
                },
                "#revision": 3,
            }
        )

        self.entity_db.delete_many_link_dps(
            ["source"],
            [["source-1"]],
            ["iterable_link"],
            [["deleted"]],
        )

        document = self.master.find_one({"_id": "source-1"})
        self.assertEqual(document["iterable_link"]["v"], [{"eid": "kept"}])
        self.assertEqual(document["#revision"], 4)

        self.entity_db.delete_link_dps("source", ["source-1"], "iterable_link", "kept")

        document = self.master.find_one({"_id": "source-1"})
        self.assertNotIn("iterable_link", document)
        self.assertEqual(document["#revision"], 5)


if __name__ == "__main__":
    unittest.main()
