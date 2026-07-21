import unittest
from datetime import UTC, datetime, timedelta
from ipaddress import IPv4Address

import common

from dp3.common.mac_address import MACAddress
from dp3.database.config import MongoConfig
from dp3.database.database import EntityDatabase
from dp3.database.snapshots import SnapshotCollectionContainer


class OversizedSnapshotStateDiscovery(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db_config = MongoConfig.model_validate(common.CONFIG.get("database", {}))
        cls.client = EntityDatabase.connect(cls.db_config)
        cls.client.admin.command("ping")
        cls.db = cls.client[cls.db_config.db_name]

    @classmethod
    def tearDownClass(cls):
        cls.client.close()

    @classmethod
    def _new_snapshot_container(cls):
        return SnapshotCollectionContainer(
            cls.db,
            cls.db_config,
            common.MODEL_SPEC,
            common.CONFIG.get("snapshots", {}),
        )

    def test_oversized_non_string_eids_are_rediscovered_after_restart(self):
        test_cases = [
            ("del_anchor_int", 918273645),
            ("del_ttl_ip", IPv4Address("198.51.100.241")),
            ("del_weak_mac", MACAddress("02:00:00:00:00:f1")),
        ]
        created_at = datetime.now(UTC).replace(microsecond=0)

        for entity_type, eid in test_cases:
            with self.subTest(entity_type=entity_type):
                snapshots = self._new_snapshot_container()[entity_type]
                snapshots._col().delete_many(snapshots._filter_from_eid(eid))
                snapshots._os_col().delete_many({"eid": eid})

                snapshot = {"eid": eid, "_time_created": created_at}
                snapshots._col().insert_one(
                    {
                        "_id": snapshots._bucket_id(eid, created_at),
                        "_time_created": created_at,
                        "oversized": True,
                        "latest": True,
                        "count": 0,
                        "last": snapshot,
                    }
                )
                snapshots._os_col().insert_one(snapshot)

                try:
                    restarted_snapshots = self._new_snapshot_container()[entity_type]
                    restarted_snapshots.save_one({"eid": eid}, created_at + timedelta(seconds=1))

                    marker = restarted_snapshots._col().find_one(
                        restarted_snapshots._filter_from_eid(eid)
                    )
                    self.assertEqual(str(marker["last"]["eid"]), str(eid))
                    self.assertEqual(restarted_snapshots._os_col().count_documents({"eid": eid}), 2)
                finally:
                    snapshots._col().delete_many(snapshots._filter_from_eid(eid))
                    snapshots._os_col().delete_many({"eid": eid})


if __name__ == "__main__":
    unittest.main()
