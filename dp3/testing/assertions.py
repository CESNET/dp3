"""Assertion helpers for DP3 module tests."""

from collections.abc import Iterable
from typing import Any

from pydantic import BaseModel

from dp3.common.datapoint import DataPointBase
from dp3.common.task import DataPointTask


class ModuleAssertions:
    """Partial-match assertions for module hook outputs."""

    def assert_no_tasks(self, tasks: Iterable[DataPointTask]) -> None:
        self.assertEqual([], list(tasks))

    def assert_task_emitted(self, tasks: Iterable[DataPointTask], **expected) -> DataPointTask:
        task_list = list(tasks)
        for task in task_list:
            if self._partial_match(dump_value(task), expected):
                return task
        self.fail(f"No emitted task matched {expected!r}. Emitted tasks: {dump_value(task_list)!r}")

    def assert_datapoint(self, tasks: Iterable[DataPointTask], **expected) -> DataPointBase:
        datapoints = list(self.iter_datapoints(tasks))
        for dp in datapoints:
            if self._partial_match(dump_value(dp), expected):
                return dp
        self.fail(
            f"No emitted datapoint matched {expected!r}. "
            f"Emitted datapoints: {dump_value(datapoints)!r}"
        )

    def assert_record_contains(self, record: dict, **expected) -> None:
        if not self._partial_match(record, expected):
            self.fail(f"Record {record!r} does not contain expected values {expected!r}")

    assertNoTasks = assert_no_tasks
    assertTaskEmitted = assert_task_emitted
    assertDatapoint = assert_datapoint
    assertRecordContains = assert_record_contains

    @staticmethod
    def iter_datapoints(tasks: Iterable[DataPointTask]) -> Iterable[DataPointBase]:
        for task in tasks:
            yield from task.data_points

    @classmethod
    def _partial_match(cls, actual: Any, expected: Any) -> bool:
        actual = dump_value(actual)
        expected = dump_value(expected)
        if isinstance(expected, dict):
            if not isinstance(actual, dict):
                return False
            return all(
                key in actual and cls._partial_match(actual[key], value)
                for key, value in expected.items()
            )
        return actual == expected


def dump_value(value: Any) -> Any:
    """Convert pydantic values recursively to plain Python containers."""
    if isinstance(value, BaseModel):
        return value.model_dump()
    if isinstance(value, list):
        return [dump_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(dump_value(item) for item in value)
    if isinstance(value, dict):
        return {key: dump_value(item) for key, item in value.items()}
    return value
