import unittest
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import common

from dp3.core.link_manager import LinkManager
from dp3.database.config import MongoConfig
from dp3.database.database import EntityDatabase
from dp3.database.encodings import get_codec_options


class IterableObservationLinks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        db_config = MongoConfig.model_validate(common.CONFIG.get("database", {}))
        cls.client = EntityDatabase.connect(db_config)
        cls.client.admin.command("ping")
        cls.db = cls.client.get_database(db_config.db_name, codec_options=get_codec_options())
        cls.cache = cls.db["#cache#IterableObservationLinkRegression"]

        cls.link_manager = LinkManager.__new__(LinkManager)
        cls.link_manager.cache = cls.cache

    @classmethod
    def tearDownClass(cls):
        cls.cache.drop()
        cls.client.close()

    def setUp(self):
        self.cache.delete_many({})

    @staticmethod
    def _datapoint(*targets):
        return SimpleNamespace(
            etype="source",
            attr="observed_targets",
            v=[SimpleNamespace(eid=target) for target in targets],
            t2=datetime(2025, 1, 1, tzinfo=UTC),
        )

    def _add_links(self, *targets):
        self.link_manager.add_iterable_observation_to_link_cache(
            "target", timedelta(hours=1), "source-1", self._datapoint(*targets)
        )

    def _cached_links(self):
        return list(self.cache.find({}, {"_id": 0, "to": 1}))

    def test_empty_cache_creates_one_link_per_target(self):
        self._add_links("a", "a", "b")

        self.assertCountEqual(self._cached_links(), [{"to": "target#a"}, {"to": "target#b"}])

    def test_existing_links_do_not_prevent_new_target_upserts(self):
        self._add_links("a")
        self._add_links("a", "b")

        self.assertCountEqual(self._cached_links(), [{"to": "target#a"}, {"to": "target#b"}])


if __name__ == "__main__":
    unittest.main()
