import json
import sys
from time import sleep
from typing import Any

import common
import requests

from dp3.api.internal.entity_response_models import EntityEidAttrValueOrHistory


def make_dps(entity_dict: dict[str, dict[str, dict[str, Any]]]) -> list[dict[str, Any]]:
    """Take a db "inventory" and produce a list of datapoints to create it."""
    dps = []
    for entity, e_dict in entity_dict.items():
        for eid, record in e_dict.items():
            for attr, value in record.items():
                dps.append(
                    {
                        "type": entity,
                        "id": eid,
                        "attr": attr,
                        "v": value,
                    }
                )
    return dps


GET_ATTR = "entity/{entity}/{eid}/get/{attr}"


def ref(link: Any):
    return {"eid": link}


class GarbageCollectorTest(common.APITest):
    test1_data = "test1_data"
    test2_data = "test2_data"
    test3_data = "test3_data"
    test4_data = "test4_data"
    test5_data = "test5_data"
    test6_data = "test6_data"
    test7_data = "test7_data"

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        d1 = "data1"  # Data attribute for all entities
        r = "ref"  # Ref attribute to weak entity for all entities

        # Simple delete test - create isolated anchor and delete via API
        del_anchor = {"a1": {d1: cls.test1_data}}
        del_anchor_int = {1: {d1: cls.test1_data}}

        # Simple weak test - create an anchor and a weak entity
        del_anchor["a2"] = {d1: cls.test2_data, r: ref("w1")}
        del_weak = {"w1": {d1: cls.test2_data}}

        del_anchor_int[2] = {d1: cls.test2_data, r: ref("00:00:00:00:00:01")}
        del_weak_mac = {"00:00:00:00:00:01": {d1: cls.test2_data}}

        # Weak chains test - an anchor and two weak entity chain
        del_anchor["a3"] = {d1: cls.test4_data, r: ref("w2_1")}
        del_weak["w2_1"] = {d1: cls.test4_data, r: ref("w2_2")}
        del_weak["w2_2"] = {d1: cls.test4_data}

        del_anchor_int[3] = {d1: cls.test4_data, r: ref("00:00:00:00:00:21")}
        del_weak_mac["00:00:00:00:00:21"] = {d1: cls.test4_data, r: ref("00:00:00:00:00:22")}
        del_weak_mac["00:00:00:00:00:22"] = {d1: cls.test4_data}

        # Weak multiple ref test - two anchors, one weak entity
        del_anchor["a4"] = {d1: cls.test5_data, r: ref("w3")}
        del_anchor["a5"] = {d1: cls.test5_data, r: ref("w3")}
        del_weak["w3"] = {d1: cls.test5_data}

        del_anchor_int[4] = {d1: cls.test5_data, r: ref("00:00:00:00:00:31")}
        del_anchor_int[5] = {d1: cls.test5_data, r: ref("00:00:00:00:00:31")}
        del_weak_mac["00:00:00:00:00:31"] = {d1: cls.test5_data}

        # No references after delete test - create an anchor and a weak entity, delete the weak
        del_anchor["a6"] = {d1: cls.test6_data, r: ref("w4")}
        del_weak["w4"] = {d1: cls.test6_data}

        del_anchor_int[6] = {d1: cls.test6_data, r: ref("00:00:00:00:00:41")}
        del_weak_mac["00:00:00:00:00:41"] = {d1: cls.test6_data}

        inventory = {
            "del_anchor": del_anchor,
            "del_anchor_int": del_anchor_int,
            "del_weak": del_weak,
            "del_weak_mac": del_weak_mac,
        }
        cls.setup_inventory(inventory)

    @classmethod
    def setup_inventory(cls, inventory: dict[str, dict[str, dict[str, Any]]]) -> None:
        dps = make_dps(inventory)
        res = cls.push_datapoints(dps)
        if res.status_code != 200:
            print(json.dumps(res.json(), indent=2), sys.stderr)
            raise Exception(f"Failed to push datapoints: {res.status_code}")
        sleep(1)  # Wait for the data to be processed

    @staticmethod
    def delete_request(entity: str, eid: str) -> requests.Response:
        return requests.delete(f"{common.base_url}/entity/{entity}/{eid}", timeout=5)

    def get_attr(self, entity: str, eid: str, attr: str) -> EntityEidAttrValueOrHistory:
        return self.get_entity_data(
            GET_ATTR.format(entity=entity, eid=eid, attr=attr),
            EntityEidAttrValueOrHistory,
        )

    def assertExpectedAttrValue(self, entity: str, eid: str, attr: str, expected: Any) -> None:
        self.query_expected_value(
            lambda: self.get_attr(entity, eid, attr),
            lambda received: received == expected,
            attempts=30,
            delay_s=0.2,
            msg=f"A value of {entity}/{eid} is not as expected",
        )

    def assertExpectedAttrValues(self, entities, attr: str, expected: Any) -> None:
        for entity, eid in entities:
            self.assertExpectedAttrValue(entity, eid, attr, expected)

    def test_simple_delete(self):
        """Delete isolated anchor via API"""
        anchor = ("del_anchor", "a1")
        int_anchor = ("del_anchor_int", 1)
        both = [anchor, int_anchor]
        # Ensure the anchor exists and is reachable via API
        expected = EntityEidAttrValueOrHistory(attr_type=1, current_value=self.test1_data)
        self.assertExpectedAttrValues(both, "data1", expected)

        # Delete the anchor
        for anchor in both:
            res = self.delete_request(*anchor)
            self.assertEqual(res.status_code, 200)

        # Ensure the anchor is deleted
        sleep(1)
        expected = EntityEidAttrValueOrHistory(attr_type=1)
        with self.subTest("String anchor deleted"):
            self.assertExpectedAttrValue(*anchor, "data1", expected)
        with self.subTest("Int anchor deleted"):
            self.assertExpectedAttrValue(*int_anchor, "data1", expected)

    def test_weak_delete(self):
        """Delete anchor to kill the weak"""
        entities = [("del_anchor", "a2"), ("del_weak", "w1")]
        typed_entities = [("del_anchor_int", 2), ("del_weak_mac", "00:00:00:00:00:01")]
        all_entities = entities + typed_entities

        # Ensure the entities exist
        expected = EntityEidAttrValueOrHistory(attr_type=1, current_value=self.test2_data)
        self.assertExpectedAttrValues(all_entities, "data1", expected)

        # Delete the anchors
        res = self.delete_request(*entities[0])
        self.assertEqual(res.status_code, 200)
        res = self.delete_request(*typed_entities[0])
        self.assertEqual(res.status_code, 200)

        # Ensure all entities are deleted
        sleep(1)
        expected = EntityEidAttrValueOrHistory(attr_type=1)
        with self.subTest("String entities deleted"):
            self.assertExpectedAttrValues(entities, "data1", expected)
        with self.subTest("Typed entities deleted"):
            self.assertExpectedAttrValues(typed_entities, "data1", expected)

    def test_ttl_deletes(self):
        # Merged two tests into one in order to avoid waiting for TTL twice
        simple_ttl = {"t1": {"data1": self.test3_data}}
        weak_inventory = {
            "del_ttl": {"t2": {"data1": self.test3_data, "ref": {"eid": "w2"}}} | simple_ttl,
            "del_weak": {"w2": {"data1": self.test3_data}},
        }
        typed_weak_inventory = {
            "del_ttl_ip": {
                "3.0.0.1": {"data1": self.test3_data},
                "3.0.0.2": {"data1": self.test3_data, "ref": ref("03:00:00:00:00:01")},
            },
            "del_weak_mac": {"03:00:00:00:00:01": {"data1": self.test3_data}},
        }
        self.setup_inventory(weak_inventory | typed_weak_inventory)
        simple_ttl_entity = ("del_ttl", "t1")
        entities = [("del_ttl", "t2"), ("del_weak", "w2")]
        typed_ttl_entity = ("del_ttl_ip", "3.0.0.1")
        typed_entities = [("del_ttl_ip", "3.0.0.2"), ("del_weak_mac", "03:00:00:00:00:01")]

        # Ensure the entity exists
        expected = EntityEidAttrValueOrHistory(attr_type=1, current_value=self.test3_data)
        self.assertExpectedAttrValue(*simple_ttl_entity, "data1", expected)
        self.assertExpectedAttrValue(*typed_ttl_entity, "data1", expected)
        self.assertExpectedAttrValues(entities, "data1", expected)
        self.assertExpectedAttrValues(typed_entities, "data1", expected)

        # Wait for TTL to expire
        sleep(9.5)

        # Ensure the entity is deleted
        expected = EntityEidAttrValueOrHistory(attr_type=1)
        with self.subTest("Simple TTL deleted"):
            self.assertExpectedAttrValue(*simple_ttl_entity, "data1", expected)
        with self.subTest("Typed TTL deleted"):
            self.assertExpectedAttrValue(*typed_ttl_entity, "data1", expected)

        # Ensure the weak entity is deleted
        with self.subTest("Weak entity deleted"):
            self.assertExpectedAttrValues(entities, "data1", expected)
        with self.subTest("Typed weak entity deleted"):
            self.assertExpectedAttrValues(typed_entities, "data1", expected)

    def test_weak_chain(self):
        # Ensure the entities exist
        entities = [("del_anchor", "a3"), ("del_weak", "w2_1"), ("del_weak", "w2_2")]
        typed_entities = [
            ("del_anchor_int", 3),
            ("del_weak_mac", "00:00:00:00:00:21"),
            ("del_weak_mac", "00:00:00:00:00:22"),
        ]
        expected = EntityEidAttrValueOrHistory(attr_type=1, current_value=self.test4_data)
        self.assertExpectedAttrValues(entities, "data1", expected)
        self.assertExpectedAttrValues(typed_entities, "data1", expected)

        # Delete the anchor
        res = self.delete_request(*entities[0])
        self.assertEqual(res.status_code, 200)
        res = self.delete_request(*typed_entities[0])
        self.assertEqual(res.status_code, 200)

        # Ensure all entities are deleted
        sleep(2)
        expected = EntityEidAttrValueOrHistory(attr_type=1)
        with self.subTest("String entities deleted"):
            self.assertExpectedAttrValues(entities, "data1", expected)
        with self.subTest("Typed entities deleted"):
            self.assertExpectedAttrValues(typed_entities, "data1", expected)

    def test_weak_multiple_ref(self):
        # Ensure the entities exist
        entities = [("del_anchor", "a4"), ("del_anchor", "a5"), ("del_weak", "w3")]
        typed_entities = [
            ("del_anchor_int", 4),
            ("del_anchor_int", 5),
            ("del_weak_mac", "00:00:00:00:00:31"),
        ]
        expected = EntityEidAttrValueOrHistory(attr_type=1, current_value=self.test5_data)
        self.assertExpectedAttrValues(entities, "data1", expected)
        self.assertExpectedAttrValues(typed_entities, "data1", expected)

        # Delete one of the anchors
        to_del = entities.pop(0)
        res = self.delete_request(*to_del)
        self.assertEqual(res.status_code, 200)
        to_del = typed_entities.pop(0)
        res = self.delete_request(*to_del)
        self.assertEqual(res.status_code, 200)

        # Ensure the weak entity is still present
        sleep(1)
        self.assertExpectedAttrValues(entities, "data1", expected)
        self.assertExpectedAttrValues(typed_entities, "data1", expected)

        # Delete the last anchor
        to_del = entities.pop(0)
        res = self.delete_request(*to_del)
        self.assertEqual(res.status_code, 200)
        to_del = typed_entities.pop(0)
        res = self.delete_request(*to_del)
        self.assertEqual(res.status_code, 200)

        # Ensure the weak entity is deleted
        sleep(1)
        expected = EntityEidAttrValueOrHistory(attr_type=1)
        with self.subTest("String entities deleted"):
            self.assertExpectedAttrValues(entities, "data1", expected)
        with self.subTest("Typed entities deleted"):
            self.assertExpectedAttrValues(typed_entities, "data1", expected)

    def test_no_ref_after_delete(self):
        entities = [("del_anchor", "a6"), ("del_weak", "w4")]
        typed_entities = [("del_anchor_int", 6), ("del_weak_mac", "00:00:00:00:00:41")]
        all_entities = entities + typed_entities

        # Ensure the entities exist
        expected = EntityEidAttrValueOrHistory(attr_type=1, current_value=self.test6_data)
        self.assertExpectedAttrValues(all_entities, "data1", expected)

        # Delete the anchors
        res = self.delete_request(*entities[1])
        self.assertEqual(res.status_code, 200)
        res = self.delete_request(*typed_entities[1])
        self.assertEqual(res.status_code, 200)

        # Ensure weak entities are deleted
        sleep(1)
        expected = EntityEidAttrValueOrHistory(attr_type=1)
        with self.subTest("String entities deleted"):
            self.assertExpectedAttrValue(*entities[1], "data1", expected)
        with self.subTest("Typed entities deleted"):
            self.assertExpectedAttrValue(*typed_entities[1], "data1", expected)

        # Ensure the anchor is still present, but has no references
        expected_ref = expected
        expected = EntityEidAttrValueOrHistory(attr_type=1, current_value=self.test6_data)
        with self.subTest("String anchor has correct value"):
            self.assertExpectedAttrValue(*entities[0], "data1", expected)
            self.assertExpectedAttrValue(*entities[0], "ref", expected_ref)
        with self.subTest("Typed anchor has correct value"):
            self.assertExpectedAttrValue(*typed_entities[0], "data1", expected)
            self.assertExpectedAttrValue(*typed_entities[0], "ref", expected_ref)

    def test_no_ref_after_garbage_collection(self):
        # No references after garbage collection test
        del_anchor = {
            "a7": {"data1": self.test7_data, "ref_ttl": ref("t5")},
            "a8": {"data1": self.test7_data, "ref_ttl": ref("t5")},
        }
        del_anchor_int = {
            7: {"data1": self.test7_data, "ref_ttl": ref("5.0.0.1")},
            8: {"data1": self.test7_data, "ref_ttl": ref("5.0.0.1")},
        }
        inventory = {
            "del_anchor": del_anchor,
            "del_ttl": {"t5": {"data1": self.test7_data}},
            "del_anchor_int": del_anchor_int,
            "del_ttl_ip": {"5.0.0.1": {"data1": self.test7_data}},
        }
        entities = [("del_anchor", "a7"), ("del_anchor", "a8"), ("del_ttl", "t5")]
        typed_entities = [("del_anchor_int", 7), ("del_anchor_int", 8), ("del_ttl_ip", "5.0.0.1")]
        all_entities = entities + typed_entities
        self.setup_inventory(inventory)

        expected_data = EntityEidAttrValueOrHistory(attr_type=1, current_value=self.test7_data)
        expected_none = EntityEidAttrValueOrHistory(attr_type=1)

        # Ensure the entities exist
        self.assertExpectedAttrValues(all_entities, "data1", expected_data)

        # Await TTL expiration
        sleep(9.5)

        # Ensure the entities are deleted
        with self.subTest("String TTL deleted"):
            self.assertExpectedAttrValue(*entities[-1], "data1", expected_none)
        with self.subTest("Typed TTL deleted"):
            self.assertExpectedAttrValue(*typed_entities[-1], "data1", expected_none)

        # Ensure the anchors are still present, but have no references
        with self.subTest("String anchors has correct value"):
            self.assertExpectedAttrValues(entities[:-1], "data1", expected_data)
            self.assertExpectedAttrValues(entities[:-1], "ref_ttl", expected_none)
        with self.subTest("Typed anchors has correct value"):
            self.assertExpectedAttrValues(typed_entities[:-1], "data1", expected_data)
            self.assertExpectedAttrValues(typed_entities[:-1], "ref_ttl", expected_none)
