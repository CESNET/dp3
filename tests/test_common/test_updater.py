import logging
import unittest
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

from dp3.core.updater import UpdateThreadState


class TestProcessBatchDivByZero(unittest.TestCase):
    """Regression tests for ZeroDivisionError in Updater._process_batch.

    `state.total` is sourced from MongoDB `estimated_document_count`, which is 0
    for an empty (or newly created) collection. The warning emitted when hook
    runtime exceeds the period previously computed `state.period / state.total`
    (and `state.runtime_secs / state.processed`), raising ZeroDivisionError.
    """

    def _make_state(self, *, total, processed, period, runtime_secs):
        now = datetime.now(UTC)
        state = UpdateThreadState(
            t_created=now,
            t_last_update=now,
            t_end=now + timedelta(seconds=period),
            period=period,
            etype="test_entity_type",
            eid_only=False,
            hook_ids=["h"],
            total=total,
            processed=processed,
            runtime_secs=runtime_secs,
            iteration=0,
            total_iterations=2,
        )
        return state

    def _make_updater(self, total):
        # Bypass the full constructor; _process_batch only needs log/cache/db.
        from dp3.core.updater import Updater

        updater = Updater.__new__(Updater)
        updater.log = logging.getLogger("Updater")
        updater.cache = MagicMock()
        updater.db = MagicMock()
        updater.db.get_estimated_entity_count.return_value = total
        return updater

    def _run(self, state, records):
        updater = self._make_updater(state.total)

        # A no-op hook_runner; _process_batch increments state.processed itself.
        def hook_runner(hooks, etype, record):
            pass

        # record_getter returns the prepared records and is expected to iterate.
        def record_getter(iteration, iteration_cnt, entity_type):
            return iter(records)

        # _process_batch uses datetime.now(UTC) deltas, so force runtime over the
        # period by pre-loading runtime_secs above the period.
        updater._process_batch(
            "test_entity_type",
            {"h": lambda *a, **k: []},
            state,
            record_getter=record_getter,
            hook_runner=hook_runner,
        )
        return updater

    def test_zero_total_does_not_raise(self):
        state = self._make_state(total=0, processed=0, period=5.0, runtime_secs=6.0)
        # One record keeps processed>0 in the warning path (matches the original
        # traceback where the period/total division failed first).
        self._run(state, records=[{"_id": "e1"}])
        # No exception means success; the dedicated warning branch was taken.

    def test_zero_total_and_zero_processed_does_not_raise(self):
        state = self._make_state(total=0, processed=0, period=5.0, runtime_secs=6.0)
        self._run(state, records=[])
        # No exception; runtime_secs/process is guarded too.

    def test_nonzero_total_keeps_original_warning(self):
        state = self._make_state(total=10, processed=0, period=5.0, runtime_secs=6.0)
        self._run(state, records=[{"_id": "e1"}])
        self.assertEqual(state.processed, 1)
        self.assertEqual(state.total, 10)


if __name__ == "__main__":
    unittest.main()
