import logging
import unittest
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import common
from repair_min_t2s import (
    fill_missing_markers_pipeline,
    inspect_entity,
    missing_marker_filter,
)

from dp3.core.updater import UpdateBatch, UpdatePeriod
from dp3.database.config import MongoConfig
from dp3.database.database import EntityDatabase
from dp3.database.encodings import get_codec_options
from dp3.history_management.history_manager import DatapointCleaningConfig, HistoryManager


class HistoryMarkers(common.APITest):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        db_config = MongoConfig.model_validate(common.CONFIG.get("database", {}))
        cls.client = EntityDatabase.connect(db_config)
        cls.client.admin.command("ping")
        cls.db = cls.client.get_database(db_config.db_name, codec_options=get_codec_options())

        cls.entity_db = EntityDatabase.bare(common.MODEL_SPEC, cls.db)

    @classmethod
    def tearDownClass(cls):
        cls.client.close()

    def setUp(self):
        self.eids = {
            "append": "history-marker-append-regression",
            "link": "history-marker-link-regression",
            "multi_link": "history-marker-multi-link-regression",
            "repair": "history-marker-repair-regression",
            "multi_cleanup": "history-marker-multi-cleanup-regression",
            "fresh": "history-marker-fresh-regression",
            "batch_0": "history-marker-batch-0-regression",
            "batch_1": "history-marker-batch-1-regression",
            "batch_2": "history-marker-batch-2-regression",
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

    def test_old_datapoint_deletion_cleans_all_histories_in_one_rewrite(self):
        now = datetime.now(UTC).replace(microsecond=0)
        t_old = now - timedelta(days=1)
        old, fresh = now - timedelta(days=2), now
        # test_attr_data_link_multi is absent from the documents and must stay absent
        t_olds = dict.fromkeys(
            [
                "test_attr_history",
                "test_attr_timeseries",
                "test_attr_multi_value",
                "test_attr_data_link_multi",
            ],
            t_old,
        )
        self.test_master.insert_many(
            [
                {
                    "_id": self.eids["multi_cleanup"],
                    "test_attr_history": [
                        {"t1": old, "t2": old, "v": 1, "c": 1.0},
                        {"t1": fresh, "t2": fresh, "v": 2, "c": 1.0},
                    ],
                    "test_attr_timeseries": [{"t1": old, "t2": old, "v": {"a": [1]}}],
                    "test_attr_multi_value": [{"t1": fresh, "t2": fresh, "v": 3, "c": 1.0}],
                    "#min_t2s": {
                        "test_attr_history": old,
                        "test_attr_timeseries": old,
                        "test_attr_multi_value": fresh,
                    },
                    "#revision": 1,
                },
                {
                    "_id": self.eids["fresh"],
                    "test_attr_history": [{"t1": fresh, "t2": fresh, "v": 1, "c": 1.0}],
                    "#min_t2s": {"test_attr_history": fresh},
                    "#revision": 1,
                },
            ]
        )

        result = self.entity_db.delete_old_dps(
            "test_entity_type",
            t_olds,
            {"_id": {"$in": [self.eids["multi_cleanup"], self.eids["fresh"]]}},
        )

        self.assertEqual(result.matched_count, 1)
        self.assertEqual(result.modified_count, 1)
        document = self.test_master.find_one({"_id": self.eids["multi_cleanup"]})
        self.assertEqual([dp["v"] for dp in document["test_attr_history"]], [2])
        self.assertNotIn("test_attr_timeseries", document)
        self.assertEqual(len(document["test_attr_multi_value"]), 1)
        self.assertNotIn("test_attr_data_link_multi", document)
        self.assertEqual(
            document["#min_t2s"], {"test_attr_history": fresh, "test_attr_multi_value": fresh}
        )
        self.assertEqual(document["#revision"], 2)
        self.assertEqual(self.test_master.find_one({"_id": self.eids["fresh"]})["#revision"], 1)

    def test_history_manager_cleans_only_records_of_updater_batch(self):
        now = datetime.now(UTC).replace(microsecond=0)
        old = now - timedelta(days=30)
        batch_eids = [self.eids["batch_0"], self.eids["batch_1"], self.eids["batch_2"]]
        self.test_master.insert_many(
            [
                {
                    "_id": eid,
                    "#hash": record_hash,
                    "test_attr_history": [
                        {"t1": old, "t2": old, "v": 1, "c": 1.0},
                        {"t1": now, "t2": now, "v": 2, "c": 1.0},
                    ],
                    "#min_t2s": {"test_attr_history": old},
                    "#revision": 1,
                }
                for eid, record_hash in zip(batch_eids, [4, 5, 6], strict=True)
            ]
        )
        history_manager = self._history_manager()
        period_start = now - timedelta(hours=1)

        metrics = history_manager._delete_old_dps_batch(
            "test_entity_type",
            UpdateBatch(
                query_filter={"#hash": {"$mod": [2, 0]}},
                iteration=0,
                total_iterations=2,
                period_start=period_start,
            ),
            max_ages={"test_attr_history": timedelta(days=7)},
        )

        self.assertEqual(metrics, {"batches": 1, "failed_batches": 0, "records": 2, "modified": 2})
        documents = {d["_id"]: d for d in self.test_master.find({"_id": {"$in": batch_eids}})}
        for eid in [self.eids["batch_0"], self.eids["batch_2"]]:
            self.assertEqual([dp["v"] for dp in documents[eid]["test_attr_history"]], [2])
            self.assertEqual(documents[eid]["#min_t2s"], {"test_attr_history": now})
            self.assertEqual(documents[eid]["#revision"], 2)
        # Records of other batches are left for their own batch
        self.assertEqual(len(documents[self.eids["batch_1"]]["test_attr_history"]), 2)
        self.assertEqual(documents[self.eids["batch_1"]]["#revision"], 1)

    def _history_manager(self) -> HistoryManager:
        history_manager = HistoryManager.__new__(HistoryManager)
        history_manager.log = logging.getLogger("HistoryManagerTest")
        history_manager.db = self.entity_db
        history_manager.config = SimpleNamespace(datapoint_cleaning=DatapointCleaningConfig())
        return history_manager

    def test_cleaning_period_metadata_is_saved_per_entity(self):
        # A period start no worker of the running test instance can share
        period_start = datetime(2001, 1, 1, tzinfo=UTC)
        metadata = self.db["#metadata"]
        metadata.delete_many({"cleaning_start": period_start})
        self.addCleanup(metadata.delete_many, {"cleaning_start": period_start})

        def end_period(etype, batches):
            self._history_manager()._end_cleaning_period(
                etype,
                UpdatePeriod(
                    period_start=period_start,
                    period_end=period_start + timedelta(days=1),
                    runtime_secs=2.0,
                    slowest_batch_secs=0.5,
                    metrics={"batches": batches, "failed_batches": 0, "records": 1, "modified": 1},
                ),
            )

        # Entity types are registered in any order and their periods may start together
        end_period("test_entity_type", 3)
        end_period("A", 4)
        # After a restart, the end of a period can be saved again with the same totals
        end_period("A", 4)

        records = {r["entity"]: r for r in metadata.find({"cleaning_start": period_start})}
        self.assertEqual(sorted(records), ["A", "test_entity_type"])
        self.assertEqual(records["test_entity_type"]["cleaning_metrics"]["batches"], 3)
        self.assertEqual(
            records["A"]["cleaning_metrics"],
            {
                "batches": 4,
                "failed_batches": 0,
                "records": 1,
                "modified": 1,
                "update_time": 2.0,
                "slowest_batch": 0.5,
            },
        )
        self.assertEqual(records["A"]["cleaning_end"], period_start + timedelta(days=1))
        self.assertEqual(records["A"]["#module"], "HistoryManager")


if __name__ == "__main__":
    unittest.main()
