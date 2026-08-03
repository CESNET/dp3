import logging
import os
import unittest
from collections import Counter
from functools import partial
from unittest.mock import patch

from dp3.common.callback_registrar import _drop_master
from dp3.common.config import ModelSpec, read_config_dir
from dp3.common.hook_telemetry import HookTelemetry
from dp3.snapshots.snapshooter import SnapShooter
from dp3.snapshots.snapshot_hooks import (
    SnapshotCorrelationHookContainer,
    SnapshotTimeseriesHookContainer,
)


class RecordingEventGroup:
    def __init__(self):
        self.counts = Counter()

    def log(self, event_id, count=1):
        self.counts[event_id] += count


class RecordingTaskQueueWriter:
    def __init__(self):
        self.tasks = []

    def put_task(self, task):
        self.tasks.append(task)


class TestSnapshotHookTelemetry(unittest.TestCase):
    def setUp(self):
        config = read_config_dir(
            os.path.join(os.path.dirname(__file__), "..", "test_config"), recursive=True
        )
        self.model_spec = ModelSpec(config.get("db_entities"))
        self.log = logging.getLogger(self.id())
        self.task_events = RecordingEventGroup()
        self.hook_events = RecordingEventGroup()

    def test_timeseries_hooks_record_tasks_failures_and_duration(self):
        hooks = SnapshotTimeseriesHookContainer(
            self.log, self.model_spec, self.task_events, self.hook_events
        )

        def create_tasks(_entity_type, _attr_type, _history):
            return [object(), object()]

        def fail(_entity_type, _attr_type, _history):
            raise RuntimeError("hook failed")

        context = ("test_entity_type", "test_attr_timeseries")
        hooks.register(create_tasks, *context)
        hooks.register(fail, *context)
        successful_prefix = hooks._hooks[context][0].metric_prefix
        failing_prefix = hooks._hooks[context][1].metric_prefix

        with patch(
            "dp3.common.hook_telemetry.perf_counter_ns",
            side_effect=[100, 125, 200, 240],
        ):
            tasks = hooks.run(*context, [])

        self.assertEqual(2, len(tasks))
        self.assertEqual(1, self.hook_events.counts[f"{successful_prefix}/executions"])
        self.assertEqual(2, self.hook_events.counts[f"{successful_prefix}/created_tasks"])
        self.assertEqual(25, self.hook_events.counts[f"{successful_prefix}/duration_ns"])
        self.assertEqual(1, self.hook_events.counts[f"{failing_prefix}/executions"])
        self.assertEqual(1, self.hook_events.counts[f"{failing_prefix}/failures"])
        self.assertEqual(40, self.hook_events.counts[f"{failing_prefix}/duration_ns"])
        self.assertEqual(1, self.task_events.counts["module_error"])

    def test_timeseries_hooks_reject_duplicate_registration(self):
        hooks = SnapshotTimeseriesHookContainer(
            self.log, self.model_spec, self.task_events, self.hook_events
        )

        def hook(_entity_type, _attr_type, _history):
            return []

        context = ("test_entity_type", "test_attr_timeseries")
        hooks.register(hook, *context)
        with self.assertRaisesRegex(ValueError, "already registered"):
            hooks.register(hook, *context)

    def test_correlation_hook_records_each_entity_execution_and_created_task(self):
        hooks = SnapshotCorrelationHookContainer(
            self.log, self.model_spec, self.task_events, self.hook_events
        )

        def create_task(_entity_type, _values, _master_record):
            return [object()]

        hooks.register(create_task, "A", [["data1"]], [["data2"]])
        registered_hook = hooks._hooks["A"][0][1]

        with patch(
            "dp3.common.hook_telemetry.perf_counter_ns",
            side_effect=[10, 15, 20, 27],
        ):
            tasks = hooks.run({("A", "a1"): {}, ("A", "a2"): {}}, {})

        prefix = registered_hook.metric_prefix
        self.assertEqual(2, len(tasks))
        self.assertEqual(2, self.hook_events.counts[f"{prefix}/executions"])
        self.assertEqual(2, self.hook_events.counts[f"{prefix}/created_tasks"])
        self.assertEqual(12, self.hook_events.counts[f"{prefix}/duration_ns"])
        self.assertIn("depends_on=A.data1", prefix)
        self.assertIn("may_change=A.data2", prefix)

    def test_correlation_hooks_reject_duplicate_registration(self):
        hooks = SnapshotCorrelationHookContainer(
            self.log, self.model_spec, self.task_events, self.hook_events
        )

        def hook(_entity_type, _values, _master_record):
            return []

        hooks.register(hook, "A", [["data1"]], [["data2"]])
        with self.assertRaisesRegex(ValueError, "already present"):
            hooks.register(hook, "A", [["data1"]], [["data2"]])

    def test_correlation_context_order_does_not_change_metric_identity(self):
        def hook(_entity_type, _values, _master_record):
            return []

        first = SnapshotCorrelationHookContainer(
            self.log, self.model_spec, self.task_events, self.hook_events
        )
        second = SnapshotCorrelationHookContainer(
            self.log, self.model_spec, self.task_events, self.hook_events
        )
        first.register(hook, "A", [["data1"], ["data2"]], [])
        second.register(hook, "A", [["data2"], ["data1"]], [])

        self.assertEqual(
            first._hooks["A"][0][1].metric_prefix,
            second._hooks["A"][0][1].metric_prefix,
        )

    def test_wrapped_partial_correlation_hooks_keep_their_identity(self):
        def hook(_context, _entity_type, _values):
            return []

        telemetry = HookTelemetry(self.hook_events)
        first = telemetry.wrap("snapshot_correlation", _drop_master(partial(hook, "first")), "A")
        second = telemetry.wrap("snapshot_correlation", _drop_master(partial(hook, "second")), "A")

        self.assertIn("partial(", first.metric_prefix)
        self.assertIn("first", first.metric_prefix)
        self.assertIn("second", second.metric_prefix)
        self.assertNotEqual(first.metric_prefix, second.metric_prefix)

    def test_snapshot_run_hooks_have_separate_families(self):
        snapshooter = object.__new__(SnapShooter)
        snapshooter.log = self.log
        snapshooter.elog = self.task_events
        snapshooter.model_spec = ModelSpec({})
        snapshooter.task_queue_writer = RecordingTaskQueueWriter()
        snapshooter.hook_telemetry = HookTelemetry(self.hook_events)
        snapshooter._init_hooks = []
        snapshooter._finalize_hooks = []

        def create_task():
            return [object()]

        snapshooter.register_run_init_hook(create_task)
        snapshooter.register_run_finalize_hook(create_task)
        with self.assertRaisesRegex(ValueError, "already registered"):
            snapshooter.register_run_init_hook(create_task)
        with self.assertRaisesRegex(ValueError, "already registered"):
            snapshooter.register_run_finalize_hook(create_task)
        init_hook = snapshooter._init_hooks[0]
        finalize_hook = snapshooter._finalize_hooks[0]

        with patch(
            "dp3.common.hook_telemetry.perf_counter_ns",
            side_effect=[100, 110, 200, 215],
        ):
            snapshooter._run_hooks(snapshooter._init_hooks)
            snapshooter._run_hooks(snapshooter._finalize_hooks)

        self.assertTrue(init_hook.metric_prefix.startswith("snapshot_run_init/"))
        self.assertTrue(finalize_hook.metric_prefix.startswith("snapshot_run_finalize/"))
        self.assertEqual(1, self.hook_events.counts[f"{init_hook.metric_prefix}/created_tasks"])
        self.assertEqual(1, self.hook_events.counts[f"{finalize_hook.metric_prefix}/created_tasks"])
        self.assertEqual(10, self.hook_events.counts[f"{init_hook.metric_prefix}/duration_ns"])
        self.assertEqual(15, self.hook_events.counts[f"{finalize_hook.metric_prefix}/duration_ns"])
        self.assertEqual(2, len(snapshooter.task_queue_writer.tasks))


if __name__ == "__main__":
    unittest.main()
