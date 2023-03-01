import datetime
import json
import logging
import os
import unittest
from functools import partial, update_wrapper

from dp3.common.config import ModelSpec, PlatformConfig, read_config_dir
from dp3.snapshots.snapshooter import SnapShooter
from dp3.snapshots.snapshot_hooks import SnapshotCorrelationHookContainer


def copy_linked(_: str, record: dict, link: str, attr: str):
    record[attr] = record[link][attr]


def modify_value(_: str, record: dict, attr: str, value):
    record[attr] = value


dummy_hook_abc = update_wrapper(partial(modify_value, attr="data2", value="abc"), modify_value)
dummy_hook_def = update_wrapper(partial(modify_value, attr="data1", value="def"), modify_value)


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
        self.container.run("A", {("A", "a1"): values})
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


class MockScheduler:
    def register(self, *_, **__):
        return None


class MockDB:
    def __init__(self, content: dict):
        self.db_content = content
        self.saved_snapshots = []

    def get_master_record(self, etype: str, ekey: str) -> dict:
        return self.db_content[etype][ekey] or {}

    def save_snapshot(self, etype: str, snapshot: dict):
        json.dumps(snapshot)
        self.saved_snapshots.append(snapshot)


class TestSnapshotOperation(unittest.TestCase):
    def setUp(self) -> None:
        config_base_path = os.path.join(os.path.dirname(__file__), "..", "test_config")
        config = read_config_dir(config_base_path, recursive=True)
        self.model_spec = ModelSpec(config.get("db_entities"))

        now = datetime.datetime.now()
        self.t1 = now - datetime.timedelta(minutes=30)
        self.t2 = now + datetime.timedelta(minutes=30)

        self.entities = {
            "B": {
                "b1": {
                    "_id": "b1",
                    "as": [{"v": "a1", "t1": self.t1, "t2": self.t2, "c": 1.0}],
                    "data1": "initb",
                    "data2": "initb",
                }
            },
            "A": {
                "a1": {
                    "_id": "a1",
                    "bs": [{"v": "b1", "t1": self.t1, "t2": self.t2, "c": 1.0}],
                    "data1": "inita",
                    "data2": "inita",
                }
            },
        }
        self.db = MockDB(content=self.entities)

        self.snapshooter = SnapShooter(
            db=self.db,
            task_queue_writer=None,
            platform_config=PlatformConfig(
                app_name="test",
                config_base_path=config_base_path,
                config=config,
                model_spec=self.model_spec,
                process_index=0,
                num_processes=1,
            ),
            scheduler=MockScheduler(),
        )
        root = logging.getLogger()
        root.setLevel("DEBUG")

    def test_reference_cycle(self):
        self.snapshooter.make_snapshot("A", self.entities["A"]["a1"])

    def test_linked_entities_loaded(self):
        def copy_linked(_: str, record: dict, link: str, data: str):
            record[data] = record[link][data]

        self.snapshooter.register_correlation_hook(
            update_wrapper(partial(copy_linked, link="bs", data="data1"), copy_linked),
            "A",
            depends_on=[["bs", "data1"]],
            may_change=[["data1"]],
        )
        self.snapshooter.register_correlation_hook(
            update_wrapper(partial(copy_linked, link="as", data="data2"), copy_linked),
            "B",
            depends_on=[["as", "data2"]],
            may_change=[["data2"]],
        )

        self.snapshooter.make_snapshot("A", self.entities["A"]["a1"])
        self.assertEqual(self.db.saved_snapshots[-1]["data1"], "initb")
        self.assertEqual(self.db.saved_snapshots[-1]["data2"], "inita")
        self.snapshooter.make_snapshot("B", self.entities["B"]["b1"])
        self.assertEqual(self.db.saved_snapshots[-1]["data1"], "initb")
        self.assertEqual(self.db.saved_snapshots[-1]["data2"], "inita")

    def test_loaded_entities_hooks_run(self):
        self.snapshooter.register_correlation_hook(
            update_wrapper(partial(modify_value, attr="data2", value="modifa"), modify_value),
            "A",
            depends_on=[],
            may_change=[["data2"]],
        )
        self.snapshooter.register_correlation_hook(
            update_wrapper(partial(copy_linked, link="bs", data="data1"), copy_linked),
            "A",
            depends_on=[["bs", "data1"]],
            may_change=[["data1"]],
        )
        self.snapshooter.register_correlation_hook(
            update_wrapper(partial(modify_value, attr="data1", value="modifb"), modify_value),
            "B",
            depends_on=[],
            may_change=[["data1"]],
        )
        self.snapshooter.register_correlation_hook(
            update_wrapper(partial(copy_linked, link="as", data="data2"), copy_linked),
            "B",
            depends_on=[["as", "data2"]],
            may_change=[["data2"]],
        )

        self.snapshooter.make_snapshot("A", self.entities["A"]["a1"])
        self.assertEqual(self.db.saved_snapshots[-1]["data1"], "modifb")
        self.assertEqual(self.db.saved_snapshots[-1]["data2"], "modifa")
        self.snapshooter.make_snapshot("B", self.entities["B"]["b1"])
        self.assertEqual(self.db.saved_snapshots[-1]["data1"], "modifb")
        self.assertEqual(self.db.saved_snapshots[-1]["data2"], "modifa")

    def test_multivalue_fitering(self):
        test_entity = {
            "_id": "t1",
            "test_attr_multi_value": [
                {"v": "a", "t1": self.t1, "t2": self.t2, "c": 1.0},
                {"v": "b", "t1": self.t1, "t2": self.t2, "c": 0.8},
                {"v": "c", "t1": self.t1, "t2": self.t2, "c": 0.4},
                {"v": "d", "t1": self.t1, "t2": self.t2, "c": 0.0},
            ],
        }
        self.snapshooter.make_snapshot("test_entity_type", test_entity)
        snapshot_result = self.db.saved_snapshots[-1]
        self.assertEqual(len(snapshot_result["test_attr_multi_value"]), 3)
        self.assertEqual(set(snapshot_result["test_attr_multi_value"]), {"a", "b", "c"})


if __name__ == "__main__":
    unittest.main()
