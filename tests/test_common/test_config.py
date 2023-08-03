import os
import unittest
from copy import deepcopy

from dp3.common.config import HierarchicalDict, read_config_dir


class TestHierarchicalDict(unittest.TestCase):
    d1 = HierarchicalDict(
        {
            "a": "a",
            "b": {
                "b1": 1,
                "b2": [1, 2, 3],
            },
            "c": {
                "c1": {
                    "c1x": "qwer",
                    "c1y": "asdf",
                },
                "c2": 2,
            },
        }
    )

    d2 = HierarchicalDict(
        {
            "b": {
                "b1": 12345,
                "b3": "BBB",
                "b4": {"b4a": None},
            },
            "c": {
                "c2": {"c20": None},
            },
            "d": "x",
            "e": {},
        }
    )

    d3 = {
        "a": "A",
    }

    d4 = [("c", ["test"]), ("d", "y")]
    d5 = {"c": {"c1": {"c1z": None}}}
    d_empty = {}

    # Expected results
    d1d2 = HierarchicalDict(
        {
            "a": "a",
            "b": {
                "b1": 12345,
                "b2": [1, 2, 3],
                "b3": "BBB",
                "b4": {"b4a": None},
            },
            "c": {
                "c1": {
                    "c1x": "qwer",
                    "c1y": "asdf",
                },
                "c2": {"c20": None},
            },
            "d": "x",
            "e": {},
        }
    )

    d2d3d4 = HierarchicalDict(
        {
            "a": "A",
            "b": {
                "b1": 12345,
                "b3": "BBB",
                "b4": {"b4a": None},
            },
            "c": ["test"],
            "d": "y",
            "e": {},
        }
    )

    d1d5 = HierarchicalDict(
        {
            "a": "a",
            "b": {
                "b1": 1,
                "b2": [1, 2, 3],
            },
            "c": {
                "c1": {
                    "c1x": "qwer",
                    "c1y": "asdf",
                    "c1z": None,
                },
                "c2": 2,
            },
        }
    )

    def test_update(self):
        # test update of HierarchicalDict
        result = deepcopy(self.d1)
        result.update(self.d2)
        self.assertEqual(result, self.d1d2, "Update of d2 by d2 failed!")

        result = deepcopy(self.d2)
        result.update(self.d3)
        result.update(self.d4)
        self.assertEqual(result, self.d2d3d4, "Update of d2 by d3 and d4 failed!")

        result = deepcopy(self.d1)
        result.update(self.d5)
        self.assertEqual(result, self.d1d5, "Update of d1 by d5 failed!")

        result = deepcopy(self.d1)
        result.update(self.d_empty)
        result.update([])
        result.update(HierarchicalDict())
        self.assertEqual(result, self.d1, "Dict has changed after update by empty dict!")

    def test_get(self):
        # test get of HierarchicalDict
        self.assertEqual(self.d1.get("b.b1"), 1, "Dot notation test in d1 failed!")
        self.assertEqual(
            self.d1d5.get("c.c1"), self.d1d5["c"]["c1"], "Dot notation test in d1d5 failed!"
        )
        self.assertEqual(self.d2d3d4.get("b.b4.b4a"), None, "Dot notation in d2d3d4 failed!")
        self.assertEqual(self.d1d2.get("c.c1.c1x"), "qwer", "Dot notation in d1d2 failed!")


class TestReadConfig(unittest.TestCase):
    test_config_dir_recursive = {
        "file1": {
            "msg_broker": {
                "host": "localhost",
                "port": 5672,
                "virtual_host": "/",
                "username": "guest",
                "password": "guest",
            },
            "worker_processes": 1,
        },
        "file2": {"mongodb": {"host": "localhost", "port": 27017, "dbname": "nerd"}},
        "modules": {
            "module1": {
                "record_life_length": {
                    "warden": 14,
                    "misp": 180,
                    "highly_active": 14,
                    "long_active": 28,
                },
                "record_life_threshold": {"highly_active": 1000, "long_active": 30},
            },
            "module2": {
                "rate-limit": {"tokens-per-sec": 1, "bucket-size": 60, "redis": {"db-index": 1}}
            },
        },
    }

    test_config_dir_nonrecursive = {
        "file1": {
            "msg_broker": {
                "host": "localhost",
                "port": 5672,
                "virtual_host": "/",
                "username": "guest",
                "password": "guest",
            },
            "worker_processes": 1,
        },
        "file2": {"mongodb": {"host": "localhost", "port": 27017, "dbname": "nerd"}},
    }

    CONFIG_TEST_DIR = "config_test_files"

    def test_read_config_dir_recursive(self):
        loaded_config = read_config_dir(
            os.path.join(os.path.dirname(__file__), self.CONFIG_TEST_DIR), recursive=True
        )
        self.assertEqual(
            self.test_config_dir_recursive,
            loaded_config,
            f"Loaded config from {self.CONFIG_TEST_DIR} directory recursively did not match!",
        )

    def test_read_config_dir_nonrecursive(self):
        loaded_config = read_config_dir(
            os.path.join(os.path.dirname(__file__), self.CONFIG_TEST_DIR)
        )
        self.assertEqual(
            self.test_config_dir_nonrecursive,
            loaded_config,
            f"Loaded config from {self.CONFIG_TEST_DIR} directory nonrecursively did not match!",
        )
