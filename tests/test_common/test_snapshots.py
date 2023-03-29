import datetime
import json
import logging
import os
import unittest
from functools import partial, update_wrapper

from dp3.common.config import ModelSpec, PlatformConfig, read_config_dir
from dp3.common.task import Task
from dp3.snapshots.snapshooter import SnapShooter
from dp3.snapshots.snapshot_hooks import SnapshotCorrelationHookContainer


def copy_linked(_: str, record: dict, link: str, attr: str):
    record[attr] = record.get(link, {}).get(attr, None)


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
        self.container.run({("A", "a1"): values})
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
        self.saved_metadata = {}

    def get_master_records(self, etype: str):
        if etype not in self.db_content:
            return []
        return self.db_content[etype].values()

    def get_master_record(self, etype: str, eid: str) -> dict:
        return self.db_content[etype][eid] or {}

    def save_snapshot(self, etype: str, snapshot: dict, time: datetime):
        json.dumps(snapshot)
        self.saved_snapshots.append(snapshot)

    def save_metadata(self, module: str, time: datetime, metadata: dict):
        self.saved_metadata[module, time] = metadata


class MockTaskQueueWriter:
    def __init__(self):
        self.tasks = []

    def put_task(self, task: Task, priority: bool = False):
        self.tasks.append(task)


class MockTaskQueueReader:
    def ack(self, msg_tag):
        ...


class TestSnapshotOperation(unittest.TestCase):
    def setUp(self) -> None:
        config_base_path = os.path.join(os.path.dirname(__file__), "..", "test_config")
        config = read_config_dir(config_base_path, recursive=True)
        self.model_spec = ModelSpec(config.get("db_entities"))

        self.now = datetime.datetime.now()
        self.t1 = self.now - datetime.timedelta(minutes=30)
        self.t2 = self.now + datetime.timedelta(minutes=30)

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
        self.task_queue_writer = MockTaskQueueWriter()
        self.snapshooter.snapshot_queue_writer = self.task_queue_writer

        self.task_queue_reader = MockTaskQueueReader()
        self.snapshooter.snapshot_queue_reader = self.task_queue_reader
        root = logging.getLogger()
        root.setLevel("DEBUG")

    def test_reference_cycle(self):
        self.snapshooter.make_snapshots()

    def test_linked_entities_loaded(self):
        self.setup_copy_linked_hooks()

        self.snapshooter.make_snapshots()
        for task in self.task_queue_writer.tasks:
            self.snapshooter.process_snapshot_task(0, task)

        for snapshot in self.db.saved_snapshots:
            self.assertEqual(snapshot["data1"], "initb")
            self.assertEqual(snapshot["data2"], "inita")

    def test_weak_component_detection(self):
        """
        Remove link from the first entity to be processed, so that an elementary BFS starting
        from this entity is not enough to load the entire weakly connected component.
        """
        self.db.db_content["A"]["a1"]["bs"] = []
        self.setup_copy_linked_hooks()

        self.snapshooter.make_snapshots()
        for value in self.db.saved_metadata.values():
            self.assertEqual(value["entities"], 2)
            self.assertEqual(value["components"], 1)

        self.assertEqual(len(self.task_queue_writer.tasks), 1)
        self.assertEqual(len(self.task_queue_writer.tasks[0].entities), 2)
        for task in self.task_queue_writer.tasks:
            self.snapshooter.process_snapshot_task(0, task)

        self.db.db_content["A"]["a1"]["bs"] = [{"v": "b1", "t1": self.t1, "t2": self.t2, "c": 1.0}]

    def test_loaded_entities_hooks_run(self):
        self.snapshooter.register_correlation_hook(
            update_wrapper(partial(modify_value, attr="data2", value="modifa"), modify_value),
            "A",
            depends_on=[],
            may_change=[["data2"]],
        )
        self.snapshooter.register_correlation_hook(
            update_wrapper(partial(modify_value, attr="data1", value="modifb"), modify_value),
            "B",
            depends_on=[],
            may_change=[["data1"]],
        )
        self.setup_copy_linked_hooks()

        self.snapshooter.make_snapshots()
        for task in self.task_queue_writer.tasks:
            self.snapshooter.process_snapshot_task(0, task)

        for snapshot in self.db.saved_snapshots:
            self.assertEqual(snapshot["data1"], "modifb")
            self.assertEqual(snapshot["data2"], "modifa")

    def setup_copy_linked_hooks(self):
        self.snapshooter.register_correlation_hook(
            update_wrapper(partial(copy_linked, link="bs", attr="data1"), copy_linked),
            "A",
            depends_on=[["bs", "data1"]],
            may_change=[["data1"]],
        )
        self.snapshooter.register_correlation_hook(
            update_wrapper(partial(copy_linked, link="as", attr="data2"), copy_linked),
            "B",
            depends_on=[["as", "data2"]],
            may_change=[["data2"]],
        )

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

        values = self.snapshooter.get_values_at_time("test_entity_type", test_entity, self.now)
        self.assertEqual(len(values["test_attr_multi_value"]), 3)
        self.assertEqual(set(values["test_attr_multi_value"]), {"a", "b", "c"})


if __name__ == "__main__":
    unittest.main()
