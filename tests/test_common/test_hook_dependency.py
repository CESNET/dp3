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

    def validate_expanded_paths(self, hook_base, path, expected_expansions):
        hook_id = self.container.register(**hook_base, may_change=[path])
        vertex = self.container._dependency_graph._vertices[hook_id]
        for expected_expansion in expected_expansions:
            self.assertIn(expected_expansion, vertex.adj)
        self.assertEqual(len(expected_expansions), len(vertex.adj))

    def test_backlinks(self):
        hook_base = {"hook": dummy_hook_abc, "entity_type": "A", "depends_on": [["data2"]]}
        paths = [
            (["bs", "as", "data1"], ["A->bs->as->data1", "A->data1"]),
            (["bs", "as", "bs", "data1"], ["A->bs->as->bs->data1", "A->bs->data1"]),
            (["bs", "cs", "ds", "cs", "data1"], ["A->bs->cs->ds->cs->data1", "A->bs->cs->data1"]),
            (
                ["bs", "as", "ds", "as", "data1"],
                ["A->bs->as->ds->as->data1", "A->bs->as->data1", "A->ds->as->data1", "A->data1"],
            ),
        ]
        for path, expected_expansions in paths:
            with self.subTest(path=path, expected_expansions=expected_expansions):
                self.validate_expanded_paths(hook_base, path, expected_expansions)

    def test_nested_backlinks(self):
        hook_base = {"hook": dummy_hook_abc, "entity_type": "A", "depends_on": [["data2"]]}
        paths = [
            (["bs", "as", "bs", "data1"], ["A->bs->as->bs->data1", "A->bs->data1"]),
            (
                ["bs", "cs", "bs", "as", "data1"],
                ["A->bs->cs->bs->as->data1", "A->data1", "A->bs->as->data1"],
            ),
            (
                ["bs", "as", "bs", "as", "data1"],
                ["A->bs->as->bs->as->data1", "A->data1", "A->bs->as->data1"],
            ),
        ]
        for path, expected_expansions in paths:
            with self.subTest(path=path, expected_expansions=expected_expansions):
                self.validate_expanded_paths(hook_base, path, expected_expansions)


if __name__ == "__main__":
    unittest.main()
