import logging
import unittest
from collections import Counter
from functools import partial
from unittest.mock import Mock, patch

from event_count_logger import DummyEventGroup

from dp3.common.attrspec import AttrType
from dp3.common.config import ModelSpec
from dp3.common.hook_telemetry import HookTelemetry
from dp3.common.utils import get_func_name, get_stable_func_name
from dp3.task_processing.task_executor import TaskExecutor
from dp3.task_processing.task_hooks import (
    TaskAttrHooksContainer,
    TaskEntityHooksContainer,
    TaskGenericHooksContainer,
)


class RecordingEventGroup:
    def __init__(self):
        self.counts = Counter()

    def log(self, event_id, count=1):
        self.counts[event_id] += count


class TestTaskHookTelemetry(unittest.TestCase):
    def setUp(self):
        self.log = logging.getLogger(self.id())
        self.task_events = RecordingEventGroup()
        self.hook_events = RecordingEventGroup()
        self.model_spec = ModelSpec({})

    @staticmethod
    def successful_task_hook(_task):
        return None

    @staticmethod
    def failing_task_hook(_task):
        raise RuntimeError("hook failed")

    def test_generic_hook_records_execution_failure_and_duration(self):
        hooks = TaskGenericHooksContainer(self.log, self.task_events, self.hook_events)
        hooks.register("on_task_start", self.successful_task_hook)
        hooks.register("on_task_start", self.failing_task_hook)
        successful_prefix = hooks._on_start[0].metric_prefix
        failing_prefix = hooks._on_start[1].metric_prefix

        with patch(
            "dp3.common.hook_telemetry.perf_counter_ns",
            side_effect=[100, 125, 200, 240],
        ):
            hooks.run_on_start(object())

        self.assertEqual(1, self.hook_events.counts[f"{successful_prefix}/executions"])
        self.assertEqual(25, self.hook_events.counts[f"{successful_prefix}/duration_ns"])
        self.assertEqual(1, self.hook_events.counts[f"{failing_prefix}/executions"])
        self.assertEqual(1, self.hook_events.counts[f"{failing_prefix}/failures"])
        self.assertEqual(40, self.hook_events.counts[f"{failing_prefix}/duration_ns"])
        self.assertEqual(1, self.task_events.counts["module_error"])

    def test_allow_creation_records_each_decision_and_short_circuits(self):
        hooks = TaskEntityHooksContainer(
            "device", self.model_spec, self.log, self.task_events, self.hook_events
        )
        later_calls = []

        def allow(_eid, _task):
            return True

        def deny(_eid, _task):
            return False

        def later(_eid, _task):
            later_calls.append(True)
            return True

        hooks.register("allow_entity_creation", allow)
        hooks.register("allow_entity_creation", deny)
        hooks.register("allow_entity_creation", later)
        allowed_prefix = hooks._allow_creation[0].metric_prefix
        denied_prefix = hooks._allow_creation[1].metric_prefix
        later_prefix = hooks._allow_creation[2].metric_prefix

        with patch(
            "dp3.common.hook_telemetry.perf_counter_ns",
            side_effect=[10, 20, 30, 50],
        ):
            result = hooks.run_allow_creation("device-1", object())

        self.assertFalse(result)
        self.assertEqual(1, self.hook_events.counts[f"{allowed_prefix}/decisions_allowed"])
        self.assertEqual(1, self.hook_events.counts[f"{denied_prefix}/decisions_denied"])
        self.assertEqual(0, self.hook_events.counts[f"{later_prefix}/executions"])
        self.assertEqual([], later_calls)

    def test_task_creating_hooks_record_returned_task_count(self):
        entity_hooks = TaskEntityHooksContainer(
            "device", self.model_spec, self.log, self.task_events, self.hook_events
        )
        attr_hooks = TaskAttrHooksContainer(
            "device",
            "hostname",
            AttrType.PLAIN,
            self.model_spec,
            self.log,
            self.task_events,
            self.hook_events,
        )
        entity_hooks.register("on_entity_creation", lambda _eid, _task: [object(), object()])
        attr_hooks.register("on_new_plain", lambda _eid, _dp: [object()])
        entity_prefix = entity_hooks._on_creation[0].metric_prefix
        attr_prefix = attr_hooks._on_new[0].metric_prefix

        with patch(
            "dp3.common.hook_telemetry.perf_counter_ns",
            side_effect=[100, 110, 200, 215],
        ):
            entity_tasks = entity_hooks.run_on_creation("device-1", object())
            attr_tasks = attr_hooks.run_on_new("device-1", object())

        self.assertEqual(2, len(entity_tasks))
        self.assertEqual(1, len(attr_tasks))
        self.assertEqual(2, self.hook_events.counts[f"{entity_prefix}/created_tasks"])
        self.assertEqual(1, self.hook_events.counts[f"{attr_prefix}/created_tasks"])

    def test_task_hook_containers_reject_duplicate_registration(self):
        containers = (
            (
                TaskGenericHooksContainer(self.log, self.task_events, self.hook_events),
                "on_task_start",
            ),
            (
                TaskEntityHooksContainer(
                    "device", self.model_spec, self.log, self.task_events, self.hook_events
                ),
                "allow_entity_creation",
            ),
            (
                TaskEntityHooksContainer(
                    "device", self.model_spec, self.log, self.task_events, self.hook_events
                ),
                "on_entity_creation",
            ),
            (
                TaskAttrHooksContainer(
                    "device",
                    "hostname",
                    AttrType.PLAIN,
                    self.model_spec,
                    self.log,
                    self.task_events,
                    self.hook_events,
                ),
                "on_new_plain",
            ),
        )

        for hooks, hook_type in containers:
            with self.subTest(hook_type=hook_type):
                hooks.register(hook_type, self.successful_task_hook)
                with self.assertRaisesRegex(ValueError, "already registered"):
                    hooks.register(hook_type, self.successful_task_hook)

    def test_colliding_telemetry_identities_share_metric_prefix(self):
        telemetry = HookTelemetry(self.hook_events)

        first = telemetry.wrap("on_task_start", self.successful_task_hook)
        second = telemetry.wrap("on_task_start", self.successful_task_hook)

        self.assertEqual(first.metric_prefix, second.metric_prefix)

    def test_wrapped_hook_is_callable_and_forwards_keyword_arguments(self):
        def callback(*, value):
            return value

        hook = HookTelemetry(self.hook_events).wrap("on_task_start", callback)

        self.assertEqual("result", hook(value="result"))
        self.assertEqual(1, self.hook_events.counts[f"{hook.metric_prefix}/executions"])

    def test_partial_identity_is_independent_of_registration_order(self):
        def callback(_context, _task):
            return None

        first = HookTelemetry(self.hook_events)
        first_a = first.wrap("on_task_start", partial(callback, "a"))
        first_b = first.wrap("on_task_start", partial(callback, "b"))
        second = HookTelemetry(self.hook_events)
        second_b = second.wrap("on_task_start", partial(callback, "b"))
        second_a = second.wrap("on_task_start", partial(callback, "a"))

        self.assertEqual(first_a.metric_prefix, second_a.metric_prefix)
        self.assertEqual(first_b.metric_prefix, second_b.metric_prefix)
        self.assertNotEqual(first_a.metric_prefix, first_b.metric_prefix)
        self.assertNotIn("/registration_", first_a.metric_prefix)
        self.assertNotIn("/registration_", first_b.metric_prefix)

    def test_callable_partial_arguments_have_stable_names(self):
        def callback(_bound, *, fallback):
            return None

        def bound():
            return None

        class BoundObject:
            pass

        name = get_stable_func_name(partial(callback, bound, BoundObject(), fallback=bound))

        self.assertNotIn("0x", name)
        self.assertEqual(2, name.count(get_stable_func_name(bound)))
        self.assertIn(f"{BoundObject.__module__}.{BoundObject.__qualname__}", name)

    def test_default_partial_names_distinguish_bound_instances(self):
        def callback(_bound):
            return None

        class BoundObject:
            pass

        first_bound = BoundObject()
        second_bound = BoundObject()
        first = get_func_name(partial(callback, first_bound))
        second = get_func_name(partial(callback, second_bound))

        self.assertNotEqual(first, second)

    def test_hook_context_uses_one_namespace_component(self):
        tracked = HookTelemetry(self.hook_events).wrap(
            "on_new_plain", self.successful_task_hook, "device/site", "hostname"
        )

        parts = tracked.metric_prefix.split("/")
        self.assertEqual(3, len(parts))
        self.assertEqual("on_new_plain", parts[0])
        self.assertIn("device%2Fsite", parts[2])

    def test_context_components_are_unambiguously_encoded(self):
        hooks = TaskEntityHooksContainer(
            "device/site", self.model_spec, self.log, self.task_events, self.hook_events
        )

        hooks.register("allow_entity_creation", lambda _eid, _task: True)

        prefix = hooks._allow_creation[0].metric_prefix
        self.assertIn("device%2Fsite", prefix)

    def test_task_hook_telemetry_is_optional(self):
        generic = TaskGenericHooksContainer(self.log, self.task_events)
        entity = TaskEntityHooksContainer("device", self.model_spec, self.log, self.task_events)
        attr = TaskAttrHooksContainer(
            "device", "hostname", AttrType.PLAIN, self.model_spec, self.log, self.task_events
        )
        executor = TaskExecutor(
            Mock(), Mock(model_spec=self.model_spec), self.task_events, self.task_events
        )

        self.assertIsInstance(generic.telemetry.event_group, DummyEventGroup)
        self.assertIsInstance(entity.telemetry.event_group, DummyEventGroup)
        self.assertIsInstance(attr.telemetry.event_group, DummyEventGroup)
        self.assertIsInstance(executor.hook_elog, DummyEventGroup)

    def test_falsy_task_hook_event_group_is_preserved(self):
        class FalsyEventGroup(RecordingEventGroup):
            def __bool__(self):
                return False

        hook_events = FalsyEventGroup()
        generic = TaskGenericHooksContainer(self.log, self.task_events, hook_events)
        executor = TaskExecutor(
            Mock(),
            Mock(model_spec=self.model_spec),
            self.task_events,
            self.task_events,
            hook_events,
        )

        self.assertIs(generic.telemetry.event_group, hook_events)
        self.assertIs(executor.hook_elog, hook_events)


if __name__ == "__main__":
    unittest.main()
