"""Recording of the worker process lifecycle in `#metadata`."""

import logging
import threading
from datetime import UTC, datetime

from dp3.database.database import EntityDatabase


class Worker:
    """Records the start and stop of the worker process in `#metadata`.

    The class name is the `#module` of the records, as `MetadataCollection.save` takes it
    from the caller, so renaming the class changes the records and breaks their readers.

    Each process saves one record under its start time, with `worker_start`, and adds
    `worker_stop` and `clean_stop` when it shuts down. `clean_stop` is true when the stop
    was requested by SIGINT or SIGTERM. A record without `worker_stop`, followed by a newer
    record of the same worker, means that the process was killed (e.g. SIGKILL, OOM,
    or a host failure).
    """

    def __init__(self, db: EntityDatabase, log: logging.Logger):
        self.db = db
        self.log = log
        self.started = datetime.now(UTC)
        self.recorded = False

    def record_start(self) -> None:
        """Saves the start record, a failure is logged and doesn't stop the worker."""
        try:
            self.db.metadata.save(self.started, {"worker_start": self.started})
            self.recorded = True
        except Exception:
            self.log.exception("Saving worker start metadata failed")

    def record_stop(self, clean: bool, timeout: float) -> None:
        """Adds the stop to the start record, if it was saved, waiting at most `timeout` seconds.

        The save runs in a daemon thread, so an unreachable database can't delay the shutdown;
        failures are logged.
        """
        if not self.recorded:
            return
        thread = threading.Thread(
            target=self._save_stop, args=(clean,), name="WorkerStopRecord", daemon=True
        )
        thread.start()
        thread.join(timeout)
        if thread.is_alive():
            self.log.error("Saving worker stop metadata did not finish in %.1f s", timeout)

    def _save_stop(self, clean: bool) -> None:
        """Saves the stop, unless the `#metadata` TTL has already removed the start record.

        The record is not recreated then, because a stop without its start would be
        an incomplete record that never expires.
        """
        try:
            found = self.db.metadata.update(
                self.started, {"worker_stop": datetime.now(UTC), "clean_stop": clean}, upsert=False
            )
        except Exception:
            self.log.exception("Saving worker stop metadata failed")
            return
        if not found:
            self.log.info("Worker start record has expired, the stop is not recorded")
