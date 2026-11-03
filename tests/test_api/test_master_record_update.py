import logging
from datetime import UTC, datetime, timedelta

import common

from dp3.database.config import MongoConfig
from dp3.database.database import EntityDatabase
from dp3.database.encodings import get_codec_options

ETYPE = "test_entity_type"
ATTR = "test_attr_multi_value"


class MasterRecordUpdate(common.APITest):
    """Revision-checked master record updates, as used by history aggregation."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        db_config = MongoConfig.model_validate(common.CONFIG.get("database", {}))
        cls.client = EntityDatabase.connect(db_config)
        cls.db = cls.client.get_database(db_config.db_name, codec_options=get_codec_options())

        cls.entity_db = EntityDatabase.__new__(EntityDatabase)
        cls.entity_db.log = logging.getLogger("MasterRecordUpdateTest")
        cls.entity_db._db_schema_config = common.MODEL_SPEC
        cls.entity_db._master_col = lambda etype, **kwargs: cls.db.get_collection(
            f"{etype}#master", **kwargs
        )

    @classmethod
    def tearDownClass(cls):
        cls.client.close()

    def setUp(self):
        self.eid = "master-record-update-regression"
        self.master = self.db[f"{ETYPE}#master"]
        self.time_created = datetime.now(UTC).replace(microsecond=0)
        self.master.delete_many({"_id": self.eid})

    def tearDown(self):
        self.master.delete_many({"_id": self.eid})

    def history(self, n: int, values=None) -> list[dict]:
        values = values or [f"value-{i}" for i in range(n)]
        return [
            {
                "t1": self.time_created + timedelta(minutes=i),
                "t2": self.time_created + timedelta(minutes=i),
                "v": v,
                "c": 1.0,
            }
            for i, v in enumerate(values)
        ]

    def insert(self, history: list[dict], revision: int | None = 3):
        doc = {"_id": self.eid, "#time_created": self.time_created, ATTR: history}
        if revision is not None:
            doc["#revision"] = revision
        self.master.insert_one(doc)

    def test_sets_fields_and_increments_revision(self):
        self.insert(self.history(3))
        new_history = self.history(2)

        matched = self.entity_db.update_master_records(
            ETYPE, [(self.eid, 3, self.time_created, {ATTR: new_history})]
        )

        self.assertEqual(matched, 1)
        doc = self.master.find_one({"_id": self.eid})
        self.assertEqual(doc[ATTR], new_history)
        self.assertEqual(doc["#revision"], 4)

    def test_values_are_stored_literally(self):
        """Stored data must never be evaluated as an aggregation expression."""
        self.insert(self.history(1))
        new_history = self.history(3, values=["$#revision", "$$ROOT", "$literal"])

        matched = self.entity_db.update_master_records(
            ETYPE, [(self.eid, 3, self.time_created, {ATTR: new_history})]
        )

        self.assertEqual(matched, 1)
        self.assertEqual(self.master.find_one({"_id": self.eid})[ATTR], new_history)

    def test_missing_revision_starts_at_one(self):
        self.insert(self.history(2), revision=None)

        matched = self.entity_db.update_master_records(
            ETYPE, [(self.eid, None, self.time_created, {ATTR: self.history(1)})]
        )

        self.assertEqual(matched, 1)
        self.assertEqual(self.master.find_one({"_id": self.eid})["#revision"], 1)

    def test_stale_revision_is_not_applied(self):
        history = self.history(3)
        self.insert(history)

        matched = self.entity_db.update_master_records(
            ETYPE, [(self.eid, 2, self.time_created, {ATTR: self.history(1)})]
        )

        self.assertEqual(matched, 0)
        doc = self.master.find_one({"_id": self.eid})
        self.assertEqual(doc[ATTR], history)
        self.assertEqual(doc["#revision"], 3)

    def test_oplog_contains_only_changed_positions(self):
        if "setName" not in self.client.admin.command("hello"):
            self.skipTest("MongoDB is not a replica set, there is no oplog")

        stored = self.history(500)
        self.insert(stored)
        merged = [dict(dp) for dp in stored[:-1]]
        merged[10]["t2"] = stored[-1]["t2"]  # last datapoint merged into the 10th

        local = self.client.get_database("local", codec_options=get_codec_options())
        oplog = local["oplog.rs"]
        last = next(oplog.find().sort("$natural", -1).limit(1))["ts"]
        matched = self.entity_db.update_master_records(
            ETYPE, [(self.eid, 3, self.time_created, {ATTR: merged})]
        )
        self.assertEqual(matched, 1)

        entries = list(
            oplog.find(
                {
                    "ts": {"$gt": last},
                    "ns": f"{self.db.name}.{ETYPE}#master",
                    "o2._id": self.eid,
                }
            )
        )
        self.assertEqual(len(entries), 1)
        diff = entries[0]["o"]["diff"]
        self.assertEqual(
            diff[f"s{ATTR}"], {"a": True, "l": 499, "s10": {"u": {"t2": merged[10]["t2"]}}}
        )
