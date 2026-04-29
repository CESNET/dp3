"""Assertion helpers for DP3 module tests."""

from collections.abc import Iterable
from typing import Any

from pydantic import BaseModel

from dp3.common.datapoint import DataPointBase
from dp3.common.task import DataPointTask

_UNSET = object()


class ModuleAssertions:
    """Partial-match assertions for module hook outputs."""

    def assert_no_tasks(self, tasks: Iterable[DataPointTask]) -> None:
        self.assertEqual([], list(tasks))

    def assert_no_datapoints(self, tasks: Iterable[DataPointTask]) -> None:
        self.assertEqual([], list(self.iter_datapoints(tasks)))

    def assert_task_emitted(  # noqa: PLR0913
        self,
        tasks: Iterable[DataPointTask],
        *,
        etype: Any = _UNSET,
        eid: Any = _UNSET,
        data_points: Any = _UNSET,
        tags: Any = _UNSET,
        ttl_tokens: Any = _UNSET,
        delete: Any = _UNSET,
    ) -> DataPointTask:
        """Assert that a task matching the supplied ``DataPointTask`` fields was emitted."""
        expected = _selected_fields(
            etype=etype,
            eid=eid,
            data_points=data_points,
            tags=tags,
            ttl_tokens=ttl_tokens,
            delete=delete,
        )
        task_list = list(tasks)
        for task in task_list:
            if self._partial_match(dump_value(task), expected):
                return task
        self.fail(f"No emitted task matched {expected!r}. Emitted tasks: {dump_value(task_list)!r}")

    def assert_datapoint(  # noqa: PLR0913
        self,
        tasks: Iterable[DataPointTask],
        *,
        etype: Any = _UNSET,
        eid: Any = _UNSET,
        attr: Any = _UNSET,
        src: Any = _UNSET,
        v: Any = _UNSET,
        c: Any = _UNSET,
        t1: Any = _UNSET,
        t2: Any = _UNSET,
    ) -> DataPointBase:
        """Assert that a datapoint matching the supplied ``DataPointBase`` fields was emitted."""
        expected = _selected_fields(
            etype=etype,
            eid=eid,
            attr=attr,
            src=src,
            v=v,
            c=c,
            t1=t1,
            t2=t2,
        )
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

    def assert_record_attr(self, record: dict, attr: str, expected: Any) -> None:
        if attr not in record:
            self.fail(f"Record {record!r} does not contain attribute {attr!r}")
        if not self._partial_match(record[attr], expected):
            self.fail(
                f"Record attribute {attr!r} value {record[attr]!r} "
                f"does not match expected value {expected!r}"
            )

    def assert_record_unchanged(self, before: dict, after: dict) -> None:
        self.assertEqual(dump_value(before), dump_value(after))

    assertNoTasks = assert_no_tasks
    assertNoDatapoints = assert_no_datapoints
    assertTaskEmitted = assert_task_emitted
    assertDatapoint = assert_datapoint
    assertRecordContains = assert_record_contains
    assertRecordAttr = assert_record_attr
    assertRecordUnchanged = assert_record_unchanged

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


def _selected_fields(**fields: Any) -> dict[str, Any]:
    return {key: value for key, value in fields.items() if value is not _UNSET}


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
