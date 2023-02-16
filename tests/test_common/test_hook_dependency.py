import logging
import os
import unittest

from dp3.common.config import ModelSpec, read_config_dir
from dp3.snapshots.snapshot_hooks import SnapshotCorrelationHookContainer


def dummy_hook_abc(_: str, values: dict):
    values["data2"] = "abc"


def dummy_hook_def(_: str, values: dict):
    values["data2"] = "def"


class TestHookDependency(unittest.TestCase):
    def setUp(self) -> None:
        config = read_config_dir(
            os.path.join(os.path.dirname(__file__), "..", "test_config"), recursive=True
        )
        self.model_spec = ModelSpec(config.get("db_entities"))
        self.container = SnapshotCorrelationHookContainer(
            log=logging.getLogger("TestLogger"), model_spec=self.model_spec
        )

    def test_basic_function(self):
        self.container.register(
            hook=dummy_hook_abc, entity_type="A", depends_on=[["data1"]], may_change=[["data2"]]
        )
        values = {}
        self.container.run("A", values)
        self.assertEqual(values["data2"], "abc")

    def test_circular_dependency_error(self):
        self.container.register(
            hook=dummy_hook_abc, entity_type="A", depends_on=[["data1"]], may_change=[["data2"]]
        )
        with self.assertRaises(expected_exception=ValueError):
            self.container.register(
                hook=dummy_hook_def, entity_type="A", depends_on=[["data2"]], may_change=[["data1"]]
            )


if __name__ == "__main__":
    unittest.main()
