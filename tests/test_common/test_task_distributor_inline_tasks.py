import logging
import unittest
from unittest.mock import Mock, patch

from dp3.task_processing.task_distributor import TaskDistributor

_MISSING = object()


class FakeTask:
    def __init__(self, name, routing_key):
        self.name = name
        self._routing_key = routing_key
        self.etype, self.eid = routing_key.split(":", maxsplit=1)

    def routing_key(self):
        return self._routing_key

    def __repr__(self):
        return f"FakeTask({self.name!r}, {self._routing_key!r})"


class FakeExecutor:
    def __init__(self, outcomes, events=None):
        self.outcomes = outcomes
        self.processed = []
        self.events = events

    def process_task(self, task):
        self.processed.append(task)
        if self.events is not None:
            self.events.append(f"process:{task.name}")
        outcome = self.outcomes.get(task, (False, []))
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def make_distributor(limit, outcomes, events=None):
    distributor = TaskDistributor.__new__(TaskDistributor)
    distributor.log = logging.getLogger("TaskDistributorInlineTest")
    distributor.max_inline_generated_tasks = limit
    distributor.task_executor = FakeExecutor(outcomes, events)
    distributor.push_new_tasks = Mock()
    if events is not None:
        distributor.push_new_tasks.side_effect = lambda tasks: events.append(
            f"publish:{','.join(task.name for task in tasks)}"
        )
    return distributor


def make_configured_distributor(max_inline_generated_tasks=_MISSING):
    def config_get(key, default=None):
        values = {
            "processing_core.msg_broker": {},
            "db_entities": {"entity": Mock()},
            "processing_core.worker_threads": 1,
        }
        if max_inline_generated_tasks is not _MISSING:
            values["processing_core.max_inline_generated_tasks"] = max_inline_generated_tasks
        return values.get(key, default)

    platform_config = Mock()
    platform_config.process_index = 0
    platform_config.num_processes = 1
    platform_config.model_spec = Mock()
    platform_config.app_name = "test"
    platform_config.config.get.side_effect = config_get

    with (
        patch("dp3.task_processing.task_distributor.TaskQueueReader"),
        patch("dp3.task_processing.task_distributor.TaskQueueWriter"),
    ):
        return TaskDistributor(Mock(), platform_config, Mock(), Mock())


class TestTaskDistributorInlineTasks(unittest.TestCase):
    def test_missing_configuration_and_zero_publish_all_tasks_once(self):
        generated = [FakeTask("first", "entity:1"), FakeTask("second", "other:2")]
        source = FakeTask("source", "entity:1")

        for configured_value in (_MISSING, 0):
            with self.subTest(configured_value=configured_value):
                distributor = make_configured_distributor(configured_value)
                distributor.task_executor = FakeExecutor({source: (False, generated)})
                distributor.push_new_tasks = Mock()

                distributor._process_task_chain(source)

                self.assertEqual(distributor.task_executor.processed, [source])
                distributor.push_new_tasks.assert_called_once_with(generated)

    def test_same_key_child_and_grandchild_run_inline(self):
        source = FakeTask("source", "entity:1")
        child = FakeTask("child", "entity:1")
        grandchild = FakeTask("grandchild", "entity:1")
        distributor = make_distributor(
            2,
            {
                source: (False, [child]),
                child: (False, [grandchild]),
            },
        )

        distributor._process_task_chain(source)

        self.assertEqual(distributor.task_executor.processed, [source, child, grandchild])
        distributor.push_new_tasks.assert_not_called()

    def test_different_entity_or_entity_type_is_never_inlined(self):
        source = FakeTask("source", "entity:1")
        other_eid = FakeTask("other-eid", "entity:2")
        other_type = FakeTask("other-type", "other:1")
        distributor = make_distributor(3, {source: (False, [other_eid, other_type])})

        distributor._process_task_chain(source)

        self.assertEqual(distributor.task_executor.processed, [source])
        distributor.push_new_tasks.assert_called_once_with([other_eid, other_type])

    def test_mixed_outputs_publish_cross_key_before_draining_inline_fifo(self):
        events = []
        source = FakeTask("source", "entity:1")
        first = FakeTask("first", "entity:1")
        cross_key = FakeTask("cross", "other:2")
        second = FakeTask("second", "entity:1")
        descendant = FakeTask("descendant", "entity:1")
        distributor = make_distributor(
            3,
            {
                source: (False, [first, cross_key, second]),
                first: (False, [descendant]),
            },
            events=events,
        )

        distributor._process_task_chain(source)

        self.assertEqual(
            events,
            [
                "process:source",
                "publish:cross",
                "process:first",
                "process:second",
                "process:descendant",
            ],
        )

    def test_fanout_above_limit_publishes_same_key_overflow_in_order(self):
        source = FakeTask("source", "entity:1")
        children = [FakeTask(f"child-{index}", "entity:1") for index in range(4)]
        distributor = make_distributor(2, {source: (False, children)})

        distributor._process_task_chain(source)

        self.assertEqual(distributor.task_executor.processed, [source, *children[:2]])
        distributor.push_new_tasks.assert_called_once_with(children[2:])

    def test_descendants_share_the_source_allowance(self):
        source = FakeTask("source", "entity:1")
        child = FakeTask("child", "entity:1")
        grandchild = FakeTask("grandchild", "entity:1")
        overflow = FakeTask("overflow", "entity:1")
        distributor = make_distributor(
            2,
            {
                source: (False, [child]),
                child: (False, [grandchild, overflow]),
            },
        )

        distributor._process_task_chain(source)

        self.assertEqual(distributor.task_executor.processed, [source, child, grandchild])
        distributor.push_new_tasks.assert_called_once_with([overflow])

    def test_inline_child_publishes_cross_key_and_overflow_outputs_in_order(self):
        source = FakeTask("source", "entity:1")
        child = FakeTask("child", "entity:1")
        cross_key = FakeTask("cross", "other:2")
        admitted = FakeTask("admitted", "entity:1")
        overflow = FakeTask("overflow", "entity:1")
        distributor = make_distributor(
            2,
            {
                source: (False, [child]),
                child: (False, [cross_key, admitted, overflow]),
            },
        )

        distributor._process_task_chain(source)

        self.assertEqual(distributor.task_executor.processed, [source, child, admitted])
        distributor.push_new_tasks.assert_called_once_with([cross_key, overflow])

    def test_inline_exception_publishes_pending_siblings_and_escapes(self):
        source = FakeTask("source", "entity:1")
        failing = FakeTask("failing", "entity:1")
        sibling = FakeTask("sibling", "entity:1")
        distributor = make_distributor(
            2,
            {
                source: (False, [failing, sibling]),
                failing: RuntimeError("inline failure"),
            },
        )

        with self.assertRaisesRegex(RuntimeError, "inline failure"):
            distributor._process_task_chain(source)

        self.assertEqual(distributor.task_executor.processed, [source, failing])
        distributor.push_new_tasks.assert_called_once_with([sibling])

    def test_invalid_configuration_fails_initialization(self):
        for value in (True, False, -1, "1", 1.0):
            with (
                self.subTest(value=value),
                self.assertRaisesRegex(ValueError, "must be a non-negative integer"),
            ):
                make_configured_distributor(value)

    def test_root_created_result_is_not_replaced_by_child_result(self):
        source = FakeTask("source", "entity:1")
        child = FakeTask("child", "entity:1")
        distributor = make_distributor(
            1,
            {
                source: (True, [child]),
                child: (False, []),
            },
        )

        created = distributor._process_task_chain(source)

        self.assertTrue(created)
        self.assertEqual(distributor.task_executor.processed, [source, child])


if __name__ == "__main__":
    unittest.main()
