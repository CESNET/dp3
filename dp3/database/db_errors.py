"""Counting of database writes that DP³ could not complete, by operation, entity and cause."""

from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager

from event_count_logger import DummyEventGroup
from pymongo.errors import BulkWriteError, DocumentTooLarge, WriteError

from dp3.common.types import EventGroupType

# Names of MongoDB write error codes; other codes are counted as `code_<n>`.
WRITE_ERROR_CAUSES = {
    10334: "document_too_large",
    17419: "document_too_large",
    11000: "duplicate_key",
}

# Cause of writes with an unknown outcome, because their write concern failed.
UNCONFIRMED = "unconfirmed"


def _root_cause(error: BaseException) -> BaseException:
    """The original exception, unwrapping DP³ wrappers raised `from` it."""
    while error.__cause__ is not None and type(error).__module__.startswith("dp3."):
        error = error.__cause__
    return error


def _code_cause(code: int | None) -> str:
    return WRITE_ERROR_CAUSES.get(code, f"code_{code}")


def _causes(error: BaseException, count: int) -> Counter:
    """Number of affected items by cause."""
    if isinstance(error, BulkWriteError):
        details = error.details or {}
        write_errors = details.get("writeErrors", [])
        causes = Counter(_code_cause(write_error.get("code")) for write_error in write_errors)
        if details.get("writeConcernErrors") and count > len(write_errors):
            # Writes not reported as failed may or may not have been applied.
            causes[UNCONFIRMED] += count - len(write_errors)
        if causes:
            return causes
    elif isinstance(error, WriteError):
        return Counter({_code_cause(error.code): count})
    elif isinstance(error, DocumentTooLarge):
        return Counter({"document_too_large": count})
    return Counter({type(error).__name__: count})


class FailedWrites:
    """Failed writes collected so that the remaining writes can still run."""

    def __init__(self):
        self.errors: list[tuple[BaseException, int]] = []

    def __bool__(self) -> bool:
        return bool(self.errors)

    def __iter__(self) -> Iterator[tuple[BaseException, int]]:
        return iter(self.errors)

    @property
    def first(self) -> BaseException:
        return self.errors[0][0]

    def add(self, error: BaseException, count: int = 1):
        """Records an error that failed `count` writes."""
        self.errors.append((error, count))

    @contextmanager
    def suppress(self, count: int = 1) -> Iterator[None]:
        """Like `contextlib.suppress`, but records the suppressed exception."""
        try:
            yield
        except Exception as e:
            self.add(e, count)


class DbErrorCounter:
    """Counts failed database writes as `<operation>/<entity>/<cause>` events.

    Many writes run outside task processing (buffered task data, snapshots, maintenance),
    so their failures are not visible in task error counters.
    """

    def __init__(self, elog: EventGroupType | None = None):
        self.elog = elog or DummyEventGroup()

    @contextmanager
    def track(self, operation: str, etype: str, count: int = 1) -> Iterator[None]:
        """Counts an exception raised by the writes in the block, then re-raises it.

        `count` is the number of items written in the block. A bulk write error is counted
        per failed item by its error code, and items whose write concern failed
        as `unconfirmed`.
        """
        try:
            yield
        except Exception as e:
            self.count(operation, etype, e, count)
            raise

    @contextmanager
    def collect(self, operation: str, etype: str, count: int = 1) -> Iterator[FailedWrites]:
        """Counts the failed writes collected in the block, when it ends.

        An exception raised by the block is counted as in `track`.
        """
        failed = FailedWrites()
        try:
            with self.track(operation, etype, count):
                yield failed
        finally:
            for error, affected in failed:
                self.count(operation, etype, error, affected)

    def count(self, operation: str, etype: str, error: BaseException, count: int = 1):
        """Counts a failed write, see `track`."""
        for cause, affected in _causes(_root_cause(error), count).items():
            self.elog.log(f"{operation}/{etype}/{cause}", affected)
