import datetime
import json
import logging
import os
import unittest
from functools import partial, update_wrapper
from typing import Callable, Optional

from event_count_logger import DummyEventGroup

from dp3.common.config import ModelSpec, PlatformConfig, read_config_dir
from dp3.common.task import Task
from dp3.snapshots.snapshooter import SnapShooter
from dp3.snapshots.snapshot_hooks import SnapshotCorrelationHookContainer


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
            log=logging.getLogger("TestLogger"), model_spec=self.model_spec, elog=DummyEventGroup()
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
            (["bs", "as", "bs", "data1"], ["B.data1"]),
            (["bs", "cs", "ds", "cs", "data1"], ["C.data1"]),
            (["bs", "as", "ds", "as", "data1"], ["A.data1"]),
        ]
        for path, expected_expansions in paths:
            with self.subTest(path=path, expected_expansions=expected_expansions):
                self.validate_expanded_paths(hook_base, path, expected_expansions)

    def test_nested_backlinks(self):
        hook_base = {"hook": dummy_hook_abc, "entity_type": "A", "depends_on": [["data2"]]}
        paths = [
            (["bs", "as", "bs", "data1"], ["B.data1"]),
            (["bs", "cs", "bs", "as", "data1"], ["A.data1"]),
        ]
        for path, expected_expansions in paths:
            with self.subTest(path=path, expected_expansions=expected_expansions):
                self.validate_expanded_paths(hook_base, path, expected_expansions)


class MockScheduler:
    def register(self, *_, **__):
        return None


class MockCursor(list):
    def close(self):
        return None


class MockDB:
    def __init__(self, content: dict, module_cache: dict):
        self.db_content = content
        self.module_cache = module_cache
        self.saved_snapshots = []
        self.saved_metadata = {}

    def get_master_records(self, etype: str, **kwargs):
        if etype not in self.db_content:
            return MockCursor([])
        return MockCursor(list(self.db_content[etype].values()))

    def get_master_record(self, etype: str, eid: str, projection=None) -> dict:
        if projection is not None:
            projection["_id"] = projection.get("_id", True)
            return {
                key: val
                for key, val in self.db_content[etype][eid].items()
                if key in projection and projection[key]
            }
        return self.db_content[etype][eid] or {}

    def register_on_entity_delete(
        self, f_one: Callable[[str, str], None], f_many: Callable[[str, list[str]], None]
    ): ...

    def get_module_cache(self, override_called_id: Optional[str] = None):
        return self.module_cache

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
    def ack(self, msg_tag): ...


class MockCollection(dict):
    def create_index(self, *args, **kwargs): ...


class MockTaskExecutor:
    elog = DummyEventGroup()

    def register_attr_hook(self, *args, **kwargs): ...


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
                    "data1": {"v": "initb", "ts_last_update": self.now},
                    "data2": {"v": "initb", "ts_last_update": self.now},
                }
            },
            "A": {
                "a1": {
                    "_id": "a1",
                    "bs": [{"v": "b1", "t1": self.t1, "t2": self.t2, "c": 1.0}],
                    "data1": {"v": "inita", "ts_last_update": self.now},
                    "data2": {"v": "inita", "ts_last_update": self.now},
                }
            },
        }
        self.module_cache = MockCollection()
        self.db = MockDB(content=self.entities, module_cache=self.module_cache)

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
