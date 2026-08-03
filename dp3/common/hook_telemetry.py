"""Telemetry helpers for registered callback hooks."""

from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from inspect import unwrap
from time import perf_counter_ns
from typing import Generic, ParamSpec, TypeVar
from urllib.parse import quote

from dp3.common.types import EventGroupType
from dp3.common.utils import get_stable_func_name

P = ParamSpec("P")
R = TypeVar("R")


@dataclass(frozen=True)
class TrackedHook(Generic[P, R]):
    """A callable hook paired with its metric prefix and event counter group."""

    callback: Callable[P, R]
    event_group: EventGroupType
    metric_prefix: str

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        """Invoke the callback and record execution, failure, and duration counters."""
        self.log("executions")
        started = perf_counter_ns()
        try:
            return self.callback(*args, **kwargs)
        except Exception:
            self.log("failures")
            raise
        finally:
            self.log("duration_ns", perf_counter_ns() - started)

    def log(self, metric: str, count: int = 1) -> None:
        """Add a hook-specific metric when its value is nonzero."""
        if count:
            self.event_group.log(f"{self.metric_prefix}/{metric}", count=count)


class HookTelemetry:
    """Wrap hooks with execution telemetry and stable metric identities."""

    def __init__(self, event_group: EventGroupType):
        self.event_group = event_group

    def wrap(self, hook_type: str, hook: Callable[P, R], *context: str) -> TrackedHook[P, R]:
        """Return a callable hook that records telemetry under a stable identity."""
        callback = unwrap(hook)
        while isinstance(callback, partial):
            callback = unwrap(callback.func)
        callback_name = get_stable_func_name(callback)
        context_name = f"({','.join(context)})"
        prefix = "/".join(
            quote(part, safe="._-(),[]") for part in (hook_type, callback_name, context_name)
        )
        return TrackedHook(hook, self.event_group, prefix)
