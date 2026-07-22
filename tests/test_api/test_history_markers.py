import unittest
from datetime import UTC, datetime, timedelta

import common
from repair_min_t2s import (
    fill_missing_markers_pipeline,
    inspect_entity,
    missing_marker_filter,
)

from dp3.database.config import MongoConfig
from dp3.database.database import EntityDatabase
from dp3.database.encodings import get_codec_options


class HistoryMarkers(common.APITest):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        db_config = MongoConfig.model_validate(common.CONFIG.get("database", {}))
        cls.client = EntityDatabase.connect(db_config)
        cls.client.admin.command("ping")
        cls.db = cls.client.get_database(db_config.db_name, codec_options=get_codec_options())

        cls.entity_db = EntityDatabase.__new__(EntityDatabase)
        cls.entity_db._db_schema_config = common.MODEL_SPEC
        cls.entity_db._master_col = lambda etype, **kwargs: cls.db.get_collection(
            f"{etype}#master", **kwargs
        )

    @classmethod
    def tearDownClass(cls):
        cls.client.close()

    def setUp(self):
        self.eids = {
            "append": "history-marker-append-regression",
            "link": "history-marker-link-regression",
            "multi_link": "history-marker-multi-link-regression",
            "cleanup": "history-marker-cleanup-regression",
            "repair": "history-marker-repair-regression",
        }
        self.test_master = self.db["test_entity_type#master"]
        self.a_master = self.db["A#master"]
        self._clear_test_data()

    def tearDown(self):
        self._clear_test_data()

    def _clear_test_data(self):
        self.test_master.delete_many({"_id": {"$in": list(self.eids.values())}})
        self.a_master.delete_many({"_id": self.eids["link"]})
        self.db["test_entity_type#raw"].delete_many({"eid": {"$in": list(self.eids.values())}})

    def test_history_append_initializes_marker_and_keeps_earliest_t2(self):
        first_t2 = datetime.now(UTC).replace(microsecond=0)
        second_t2 = first_t2 + timedelta(minutes=1)
        first = {
            "type": "test_entity_type",
            "id": self.eids["append"],
            "attr": "test_attr_history",
            "v": 1,
            "t1": first_t2.isoformat(),
            "t2": first_t2.isoformat(),
        }
        second = first | {
            "v": 2,
            "t1": second_t2.isoformat(),
            "t2": second_t2.isoformat(),
        }

        self.assertEqual(self.push_datapoints([first]).status_code, 200)
        self.query_expected_value(
            lambda: self.test_master.find_one({"_id": self.eids["append"]}),
            lambda doc: doc is not None
            and doc.get("#min_t2s", {}).get("test_attr_history") == first_t2,
            attempts=40,
            delay_s=0.1,
        )

        self.assertEqual(self.push_datapoints([second]).status_code, 200)
        document = self.query_expected_value(
            lambda: self.test_master.find_one({"_id": self.eids["append"]}),
            lambda doc: doc is not None and len(doc.get("test_attr_history", [])) == 2,
            attempts=40,
            delay_s=0.1,
        )
        self.assertEqual(document["#min_t2s"]["test_attr_history"], first_t2)

    def test_link_deletion_recomputes_and_removes_marker(self):
        first_t2 = datetime.now(UTC).replace(microsecond=0)
        second_t2 = first_t2 + timedelta(minutes=1)
        attr = "bs"
        self.a_master.insert_one(
            {
                "_id": self.eids["link"],
                attr: [
                    {"t1": first_t2, "t2": first_t2, "v": {"eid": "target-1"}, "c": 1.0},
                    {
                        "t1": second_t2,
                        "t2": second_t2,
                        "v": {"eid": "target-2"},
                        "c": 1.0,
                    },
                ],
                "#min_t2s": {attr: first_t2},
                "#revision": 1,
            }
        )

        self.entity_db.delete_link_dps("A", [self.eids["link"]], attr, "target-1")

        document = self.a_master.find_one({"_id": self.eids["link"]})
        self.assertEqual([dp["v"]["eid"] for dp in document[attr]], ["target-2"])
        self.assertEqual(document["#min_t2s"][attr], second_t2)
        self.assertEqual(document["#revision"], 2)

        self.entity_db.delete_link_dps("A", [self.eids["link"]], attr, "target-2")

        document = self.a_master.find_one({"_id": self.eids["link"]})
        self.assertNotIn(attr, document)
        self.assertNotIn(attr, document.get("#min_t2s", {}))
        self.assertEqual(document["#revision"], 3)

    def test_multivalue_link_deletion_recomputes_marker(self):
        first_t2 = datetime.now(UTC).replace(microsecond=0)
        second_t2 = first_t2 + timedelta(minutes=1)
        attr = "test_attr_data_link_multi"
        self.test_master.insert_one(
            {
                "_id": self.eids["multi_link"],
                attr: [
                    {
                        "t1": first_t2,
                        "t2": first_t2,
                        "v": [
                            {"eid": "target-1", "data": 1},
                            {"eid": "target-2", "data": 2},
                        ],
                        "c": 1.0,
                    },
                    {
                        "t1": second_t2,
                        "t2": second_t2,
                        "v": [{"eid": "target-3", "data": 3}],
                        "c": 1.0,
                    },
                ],
                "#min_t2s": {attr: first_t2},
                "#revision": 1,
            }
        )

        self.entity_db.delete_link_dps(
            "test_entity_type", [self.eids["multi_link"]], attr, "target-2"
        )

        document = self.test_master.find_one({"_id": self.eids["multi_link"]})
        self.assertEqual(document[attr][0]["v"], [{"eid": "target-3", "data": 3}])
        self.assertEqual(document["#min_t2s"][attr], second_t2)

    def test_repair_fills_multiple_missing_markers_once(self):
        first_t2 = datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=1)
        second_t2 = first_t2 + timedelta(minutes=1)
        attrs = ["test_attr_history", "test_attr_timeseries"]
        self.test_master.insert_one(
            {
                "_id": self.eids["repair"],
                attrs[0]: [
                    {"t1": second_t2, "t2": second_t2, "v": 2, "c": 1.0},
                    {"t1": first_t2, "t2": first_t2, "v": 1, "c": 1.0},
                ],
                attrs[1]: [
                    {"t1": second_t2, "t2": second_t2, "v": {"value": [2]}},
                    {"t1": first_t2, "t2": first_t2, "v": {"value": [1]}},
                ],
                "#revision": 4,
            }
        )

        documents, counts = inspect_entity(self.test_master, attrs)
        self.assertEqual(documents, 1)
        self.assertEqual(counts, dict.fromkeys(attrs, 1))

        result = self.test_master.update_many(
            {"$or": [missing_marker_filter(attr) for attr in attrs]},
            fill_missing_markers_pipeline(attrs),
        )

        self.assertEqual(result.modified_count, 1)
        document = self.test_master.find_one({"_id": self.eids["repair"]})
        self.assertEqual(document["#min_t2s"], dict.fromkeys(attrs, first_t2))
        self.assertEqual(document["#revision"], 5)
        self.assertEqual(inspect_entity(self.test_master, attrs)[0], 0)

    def test_old_datapoint_deletion_removes_empty_history(self):
        attr = "test_attr_history"
        t2 = datetime.now(UTC).replace(microsecond=0) - timedelta(days=2)
        self.test_master.insert_one(
            {
                "_id": self.eids["cleanup"],
                attr: [{"t1": t2, "t2": t2, "v": 1, "c": 1.0}],
                "#min_t2s": {attr: t2},
                "#revision": 1,
            }
        )

        result = self.entity_db.delete_old_dps(
            "test_entity_type", attr, datetime.now(UTC).replace(microsecond=0)
        )

        self.assertGreaterEqual(result.modified_count, 1)
        document = self.test_master.find_one({"_id": self.eids["cleanup"]})
        self.assertNotIn(attr, document)
        self.assertNotIn(attr, document.get("#min_t2s", {}))
        self.assertEqual(document["#revision"], 2)


if __name__ == "__main__":
    unittest.main()
