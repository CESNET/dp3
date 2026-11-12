"""Operational records of the platform modules, kept in the `#metadata` collection."""

import inspect
import logging
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from datetime import UTC, datetime

import pymongo
from pymongo.cursor import Cursor
from pymongo.database import Database

from dp3.common.utils import error_summary
from dp3.database.exceptions import DatabaseError

METADATA_COLLECTION = "#metadata"
# Records expire 30 days after their creation
METADATA_TTL = 60 * 60 * 24 * 30


def get_caller_id():
    """Returns the name of the caller method's class, or function name if caller is not a method.

    The caller is the frame two levels up, i.e. the code calling the function that calls this.
    """
    caller = inspect.stack()[2]
    if module := caller.frame.f_locals.get("self"):
        return module.__class__.__qualname__
    return caller.function


class MetadataCollection:
    """Saves, updates and finds the records of platform modules in `#metadata`.

    Each record belongs to the module (class) that saves it, taken from the caller,
    so the methods must be called directly from a method of that class.
    A record is identified by the module, the start time of what it describes
    and the index of the worker process.
    """

    def __init__(self, db: Database, process_index: int):
        self._col = db[METADATA_COLLECTION]
        self._process_index = process_index
        self.log = logging.getLogger("EntityDatabase.Metadata")

    def create_indexes(self):
        self._col.create_index("#time_created", expireAfterSeconds=METADATA_TTL, background=True)

    def save(self, time: datetime, metadata: dict, worker_id: int | None = None):
        """Saves metadata dict under the caller module and passed timestamp."""
        self._save(get_caller_id(), time, metadata, worker_id)

    def _save(self, module: str, time: datetime, metadata: dict, worker_id: int | None = None):
        metadata["_id"] = self._record_id(module, time, worker_id)
        metadata["#module"] = module
        metadata["#time_created"] = time
        metadata["#last_update"] = datetime.now(UTC)
        try:
            self._col.insert_one(metadata)
            self.log.debug("Inserted metadata %s: %s", metadata["_id"], metadata)
        except Exception as e:
            raise DatabaseError(f"Insert of metadata failed: {e}\n{metadata}") from e

    def update(
        self,
        time: datetime,
        metadata: dict,
        increase: dict = None,
        worker_id: int | None = None,
        key: str | None = None,
        upsert: bool = True,
    ) -> bool:
        """Updates existing metadata of caller module and passed timestamp.

        The optional `key` identifies one of several records of the same module, time and worker.
        If the record does not exist yet, it is created, unless `upsert` is false.

        Returns:
            Whether the record existed before the update.
        """
        return self._update(get_caller_id(), time, metadata, increase, worker_id, key, upsert)

    def record_run_end(
        self, time: datetime, end_field: str, increase: dict = None
    ) -> AbstractContextManager[dict]:
        """Records the end of a run started by `save`, also when the run fails.

        Used as `with db.metadata.record_run_end(start, "job_end", increase=counts) as end:`.
        On exit, the time is saved as `end_field`, together with the fields put in `end`
        and the final values of the `increase` counters. A failed run also records
        an `error` field and the exception is re-raised. Failures of the write itself
        are only logged, so they never hide the outcome of the run.
        """
        return self._run_end_record(get_caller_id(), time, end_field, increase)

    def record_run(
        self, time: datetime, start: dict, end_field: str, increase: dict = None
    ) -> AbstractContextManager[dict]:
        """Records a run of the caller module, from its start to its end.

        Used as `with db.metadata.record_run(start, {"job_start": start} | counts, "job_end",
        increase=counts) as end:`. The `start` record is saved on entry, the end is recorded
        on exit as by `record_run_end`.

        The run never depends on its record: when the start can't be saved, the failure
        is logged, the run goes on and its end is not recorded either.
        """
        return self._run_record(get_caller_id(), time, start, end_field, increase)

    def find(
        self,
        module: str = None,
        date_from: datetime = None,
        date_to: datetime = None,
        newest_first: bool = True,
        extra_filter: dict = None,
    ) -> Cursor:
        """Find metadata documents with optional filtering."""
        query = dict(extra_filter or {})
        if module is not None:
            query["#module"] = module

        time_filter = {}
        if date_from is not None:
            time_filter["$gte"] = date_from
        if date_to is not None:
            time_filter["$lte"] = date_to
        if time_filter:
            query["#time_created"] = time_filter

        sort_dir = pymongo.DESCENDING if newest_first else pymongo.ASCENDING
        return self._col.find(query).sort([("#time_created", sort_dir), ("_id", sort_dir)])

    def _record_id(
        self, module: str, time: datetime, worker_id: int | None = None, key: str | None = None
    ) -> str:
        """Generates unique metadata id based on `module`, `time` and the worker index.

        The optional `key` distinguishes records of the same module, time and worker,
        e.g. of different entity types.
        """
        worker_id = self._process_index if worker_id is None else worker_id
        module_key = module if key is None else f"{module}[{key}]"
        return f"{module_key}{time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')}w{worker_id}"

    def _update(
        self,
        module: str,
        time: datetime,
        metadata: dict,
        increase: dict = None,
        worker_id: int | None = None,
        key: str | None = None,
        upsert: bool = True,
    ) -> bool:
        metadata_id = self._record_id(module, time, worker_id, key)
        metadata["#last_update"] = datetime.now(UTC)

        changes = {"$set": metadata} if increase is None else {"$set": metadata, "$inc": increase}

        try:
            res = self._col.update_one({"_id": metadata_id}, changes, upsert=upsert)
            self.log.debug(
                "Updated metadata %s, changes: %s, result: %s", metadata_id, changes, res.raw_result
            )
        except Exception as e:
            raise DatabaseError(f"Update of metadata failed: {e}\n{metadata_id}, {changes}") from e
        return res.matched_count > 0

    @contextmanager
    def _run_record(
        self, module: str, time: datetime, start: dict, end_field: str, increase: dict
    ) -> Iterator[dict]:
        try:
            self._save(module, time, start)
        except DatabaseError:
            self.log.exception("Saving the start of a %s run failed, it is not recorded", module)
            yield {}
            return
        with self._run_end_record(module, time, end_field, increase) as end:
            yield end

    @contextmanager
    def _run_end_record(
        self, module: str, time: datetime, end_field: str, increase: dict
    ) -> Iterator[dict]:
        end = {}
        try:
            yield end
        except Exception as e:
            end["error"] = error_summary(e)
            raise
        finally:
            end[end_field] = datetime.now(UTC)
            try:
                self._update(module, time, end, increase)
            except DatabaseError:
                self.log.exception("Saving the end of a %s run failed", module)
