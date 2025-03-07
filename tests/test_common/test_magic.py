"""Test the search & replace functionality for snapshot generic filter endpoint"""

import unittest
from datetime import datetime, timezone
from ipaddress import IPv4Address, IPv6Address

from bson import Binary
from bson.binary import BINARY_SUBTYPE

from dp3.common.mac_address import MACAddress
from dp3.database.magic import search_and_replace


class TestSearchAndReplace(unittest.TestCase):
    def test_no_match(self):
        query = {"ip_attr": "normal_value"}
        expected = {"ip_attr": "normal_value"}
        self.assertEqual(search_and_replace(query), expected)

    def test_replace_ipv4(self):
        query = {"ip_attr": "$$IPv4{127.0.0.1}"}
        expected = {"ip_attr": IPv4Address("127.0.0.1")}
        self.assertEqual(search_and_replace(query), expected)

    def test_replace_ipv6(self):
        query = {"ip_attr": "$$IPv6{::1}"}
        expected = {"ip_attr": IPv6Address("::1")}
        self.assertEqual(search_and_replace(query), expected)

    def test_replace_int(self):
        query = {"int_attr": "$$int{42}"}
        expected = {"int_attr": 42}
        self.assertEqual(search_and_replace(query), expected)

    def test_replace_date(self):
        query = {"date_attr": "$$Date{2021-01-01T00:00:00Z}"}
        expected = {"date_attr": datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc)}
        self.assertEqual(search_and_replace(query), expected)

    def test_replace_date_ts(self):
        query = {"date_attr": "$$DateTs{1609459200}"}
        expected = {"date_attr": datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc)}
        self.assertEqual(search_and_replace(query), expected)

        # Test with float value
        query = {"date_attr": "$$DateTs{1609459200.5}"}
        expected = {"date_attr": datetime(2021, 1, 1, 0, 0, 0, 500000, tzinfo=timezone.utc)}
        self.assertEqual(search_and_replace(query), expected)

    def test_replace_ipv4_prefix(self):
        query = {"ip_attr": "$$IPv4Prefix{127.0.0.0/24}"}
        expected = {
            "ip_attr": {"$gte": IPv4Address("127.0.0.0"), "$lte": IPv4Address("127.0.0.255")}
        }
        self.assertEqual(search_and_replace(query), expected)

    def test_replace_ipv6_prefix(self):
        query = {"ip_attr": "$$IPv6Prefix{fe80::/64}"}
        expected = {
            "ip_attr": {
                "$gte": IPv6Address("fe80::"),
                "$lte": IPv6Address("fe80::ffff:ffff:ffff:ffff"),
            }
        }
        self.assertEqual(search_and_replace(query), expected)

    def test_replace_binary_id(self):
        query = {"_id": "$$Binary_ID{$$IPv4{127.0.0.1}}"}
        expected = {
            "_id": {
                "$gte": Binary(b"\x7f\x00\x00\x01" + (b"\x00" * 8), subtype=BINARY_SUBTYPE),
                "$lt": Binary(b"\x7f\x00\x00\x01" + (b"\xff" * 8), subtype=BINARY_SUBTYPE),
            }
        }
        self.assertEqual(search_and_replace(query), expected)

    def test_replace_mac(self):
        query = {"mac_attr": "$$MAC{00:11:22:33:44:55}"}
        expected = {"mac_attr": MACAddress("00:11:22:33:44:55")}
        self.assertEqual(search_and_replace(query), expected)

    def test_replace_binary_id_mac(self):
        query = {"_id": "$$Binary_ID{$$MAC{00:11:22:33:44:55}}"}
        expected = {
            "_id": {
                "$gte": Binary(b"\x00\x11\x22\x33\x44\x55" + (b"\x00" * 8), subtype=BINARY_SUBTYPE),
                "$lt": Binary(b"\x00\x11\x22\x33\x44\x55" + (b"\xff" * 8), subtype=BINARY_SUBTYPE),
            }
        }
        self.assertEqual(search_and_replace(query), expected)

    def test_nested_query(self):
        query = {"outer": {"inner": "$$IPv4{127.0.0.1}"}}
        expected = {"outer": {"inner": IPv4Address("127.0.0.1")}}
        self.assertEqual(search_and_replace(query), expected)

    def test_nested_query_list(self):
        query = {"outer": [{"inner": "$$IPv4{192.168.0.1}"}]}
        expected = {"outer": [{"inner": IPv4Address("192.168.0.1")}]}
        self.assertEqual(search_and_replace(query), expected)

    def test_invalid_type(self):
        query = {"invalid": "$$invalid{42}"}
        query_copy = query.copy()
        self.assertEqual(search_and_replace(query), query_copy)

    def test_invalid_sub_type(self):
        invalid_queries = [
            {"invalid": "$$IPv4{invalid}"},
            {"invalid": "$$IPv4Prefix{invalid}"},
            {"invalid": "$$IPv6{invalid}"},
            {"invalid": "$$IPv6Prefix{invalid}"},
            {"invalid": "$$int{invalid}"},
            {"invalid": "$$Date{24-12-2021}"},
            {"invalid": "$$DateTs{invalid}"},
            {"invalid": "$$MAC{invalid}"},
            {"invalid": "$$Binary_ID{invalid}"},
            {"invalid": "$$Binary_ID{$$IPv4{invalid}}"},
        ]
        for query in invalid_queries:
            with self.assertRaises(ValueError):
                search_and_replace(query)

    def test_empty_binary_id(self):
        query = {"_id": "$$Binary_ID{}"}
        query_copy = query.copy()
        # Search and replace should not match
        self.assertEqual(search_and_replace(query), query_copy)


if __name__ == "__main__":
    unittest.main()
