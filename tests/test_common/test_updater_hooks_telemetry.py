import logging
import unittest
from collections import Counter, defaultdict
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import patch

from dp3.common.config import ModelSpec
from dp3.common.hook_telemetry import HookTelemetry
from dp3.core.updater import Updater


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


class TestUpdaterHookTelemetry(unittest.TestCase):
    def setUp(self):
        self.task_events = RecordingEventGroup()
        self.hook_events = RecordingEventGroup()
        self.updater = object.__new__(Updater)
        self.updater.log = logging.getLogger(self.id())
        self.updater.elog = self.task_events
        self.updater.hook_telemetry = HookTelemetry(self.hook_events)
        self.updater.model_spec = ModelSpec({})
        self.updater.task_queue_writer = RecordingTaskQueueWriter()
        self.updater.enabled = True
        self.updater.config = SimpleNamespace(update_batch_period=timedelta(seconds=5))
        self.updater.update_thread_hooks = defaultdict(dict)

    def test_record_hooks_record_tasks_failures_and_duration(self):
        def create_tasks(_entity_type, _eid, _record):
            return [object(), object()]

        def fail(_entity_type, _eid, _record):
            raise RuntimeError("hook failed")

        self.updater._register_hook(create_tasks, "create", "device", 60, eid_only=False)
        self.updater._register_hook(fail, "fail", "device", 60, eid_only=False)
        hooks = self.updater.update_thread_hooks[60, "device", False]
        successful_hook = hooks["create"]
        failing_hook = hooks["fail"]

        with patch(
            "dp3.common.hook_telemetry.perf_counter_ns",
            side_effect=[100, 125, 200, 240],
        ):
            self.updater._run_hooks(hooks, "device", {"_id": "device-1"})

        successful_prefix = successful_hook.metric_prefix
        failing_prefix = failing_hook.metric_prefix
        self.assertTrue(successful_prefix.startswith("periodic_update/"))
        self.assertIn("(device,create,period%3D60s)", successful_prefix)
        self.assertEqual(1, self.hook_events.counts[f"{successful_prefix}/executions"])
        self.assertEqual(2, self.hook_events.counts[f"{successful_prefix}/created_tasks"])
        self.assertEqual(25, self.hook_events.counts[f"{successful_prefix}/duration_ns"])
        self.assertEqual(1, self.hook_events.counts[f"{failing_prefix}/executions"])
        self.assertEqual(1, self.hook_events.counts[f"{failing_prefix}/failures"])
        self.assertEqual(40, self.hook_events.counts[f"{failing_prefix}/duration_ns"])
        self.assertEqual(1, self.task_events.counts["module_error"])
        self.assertEqual(2, len(self.updater.task_queue_writer.tasks))

    def test_eid_hooks_use_a_separate_family(self):
        def create_task(_entity_type, _eid):
            return [object()]

        self.updater._register_hook(create_task, "refresh", "device", 120, eid_only=True)
        hooks = self.updater.update_thread_hooks[120, "device", True]
        hook = hooks["refresh"]

        with patch(
            "dp3.common.hook_telemetry.perf_counter_ns",
            side_effect=[10, 25],
        ):
            self.updater._run_hooks_eid(hooks, "device", {"_id": "device-1"})

        prefix = hook.metric_prefix
        self.assertTrue(prefix.startswith("periodic_eid_update/"))
        self.assertIn("(device,refresh,period%3D120s)", prefix)
        self.assertEqual(1, self.hook_events.counts[f"{prefix}/executions"])
        self.assertEqual(1, self.hook_events.counts[f"{prefix}/created_tasks"])
        self.assertEqual(15, self.hook_events.counts[f"{prefix}/duration_ns"])
        self.assertEqual(1, len(self.updater.task_queue_writer.tasks))

    def test_period_is_part_of_the_metric_context(self):
        def hook(_entity_type, _eid):
            return []

        self.updater._register_hook(hook, "refresh", "device", 60, eid_only=True)
        self.updater._register_hook(hook, "refresh", "device", 120, eid_only=True)

        first = self.updater.update_thread_hooks[60, "device", True]["refresh"]
        second = self.updater.update_thread_hooks[120, "device", True]["refresh"]
        self.assertNotEqual(first.metric_prefix, second.metric_prefix)


if __name__ == "__main__":
    unittest.main()
