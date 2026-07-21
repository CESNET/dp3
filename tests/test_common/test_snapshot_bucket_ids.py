import unittest
from datetime import UTC, datetime
from ipaddress import IPv4Address, IPv6Address

from bson import BSON

from dp3.common.mac_address import MACAddress
from dp3.database.snapshots import (
    IntEidSnapshots,
    IPv4EidSnapshots,
    IPv6EidSnapshots,
    MACAddressEidSnapshots,
)


class TestBinarySnapshotBucketIds(unittest.TestCase):
    def test_eid_round_trip(self):
        test_cases = [
            (IntEidSnapshots, 0),
            (IntEidSnapshots, 918273645),
            (IntEidSnapshots, -918273645),
            (IPv4EidSnapshots, IPv4Address("198.51.100.241")),
            (IPv6EidSnapshots, IPv6Address("2001:db8::f1")),
            (MACAddressEidSnapshots, MACAddress("02:00:00:00:00:f1")),
        ]
        created_at = datetime(2025, 1, 2, 3, 4, 5, tzinfo=UTC)

        for collection_type, eid in test_cases:
            with self.subTest(collection_type=collection_type.__name__, eid=eid):
                collection = collection_type.__new__(collection_type)
                bucket_id = collection._bucket_id(eid, created_at)
                stored_bucket_id = BSON.decode(BSON.encode({"_id": bucket_id}))["_id"]

                self.assertEqual(collection._eid_from_bid(stored_bucket_id), eid)


if __name__ == "__main__":
    unittest.main()
