"""Test the MAC address parsing of MACAddress class"""

import json
import unittest

from pydantic import BaseModel

from dp3.common.mac_address import MACAddress


class TestModel(BaseModel):
    mac: MACAddress


class TestMACAddress(unittest.TestCase):
    def test_parse_mac_valid(self):
        self.assertEqual(MACAddress._parse_mac("01:23:45:67:89:ab"), b"\x01\x23\x45\x67\x89\xab")
        self.assertEqual(MACAddress._parse_mac("01-23-45-67-89-ab"), b"\x01\x23\x45\x67\x89\xab")
        self.assertEqual(MACAddress._parse_mac("01:23:45:67:89:AB"), b"\x01\x23\x45\x67\x89\xab")
        self.assertEqual(MACAddress._parse_mac("01-23-45-67-89-Ab"), b"\x01\x23\x45\x67\x89\xab")

    def test_parse_mac_invalid_length(self):
        with self.assertRaises(ValueError):
            MACAddress._parse_mac("01:23:45:67:89")
        with self.assertRaises(ValueError):
            MACAddress._parse_mac("01:23:45:67:89:ab:cd")

    def test_parse_mac_invalid_format(self):
        invalid_macs = [
            "01:23:45:67:89:ab:",
            "01:23-45:67-89:ab",
            "01:23:45:67:89:ag",
            "01:23:45:67:89:ab:cd",
            "",
        ]
        for mac in invalid_macs:
            with self.assertRaises(ValueError):
                MACAddress._parse_mac(mac)

    def test_model_valid(self):
        model = TestModel(mac="01:23:45:67:89:ab")
        self.assertEqual(model.mac.packed, b"\x01\x23\x45\x67\x89\xab")

        model = TestModel(mac=MACAddress("01:23:45:67:89:ab"))
        self.assertEqual(model.mac.packed, b"\x01\x23\x45\x67\x89\xab")

        model = TestModel.model_validate({"mac": b"\x01\x23\x45\x67\x89\xab"})
        self.assertEqual(model.mac.packed, b"\x01\x23\x45\x67\x89\xab")

        model = TestModel.model_validate({"mac": b"01:23:45:67:89:ab"})
        self.assertEqual(model.mac.packed, b"\x01\x23\x45\x67\x89\xab")

        model = TestModel.model_validate_json('{"mac": "01:23:45:67:89:ab"}')
        self.assertEqual(model.mac.packed, b"\x01\x23\x45\x67\x89\xab")

    def test_model_invalid(self):
        with self.assertRaises(ValueError):
            TestModel.model_validate({"mac": "01:23:45:67:89"})

        with self.assertRaises(ValueError):
            TestModel.model_validate({"mac": 420})

        with self.assertRaises(ValueError):
            TestModel.model_validate_json('{"mac": "01:23:45:67:89"}')

    def test_model_serialization(self):
        model = TestModel(mac="01:23:45:67:89:ab")
        self.assertEqual(model.model_dump(), {"mac": MACAddress("01:23:45:67:89:ab")})

        model = TestModel(mac=b"\x01\x23\x45\x67\x89\xab")
        self.assertEqual(json.loads(model.model_dump_json()), {"mac": "01:23:45:67:89:ab"})
