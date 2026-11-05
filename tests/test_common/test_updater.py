import logging
import unittest
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

from dp3.core.updater import Updater, UpdaterCache, UpdateThreadState


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


class TestBatchHooks(unittest.TestCase):
    def _make_updater(self, on_period_end=None):
        updater = Updater.__new__(Updater)
        updater.log = logging.getLogger("Updater")
        updater.elog = MagicMock()
        updater.cache = MagicMock()
        updater.db = MagicMock()
        updater.db.get_estimated_entity_count.return_value = 10
        updater._period_end_callbacks = defaultdict(dict)
        if on_period_end is not None:
            thread_id = (300.0, "test_entity_type", False, True)
            updater._period_end_callbacks[thread_id]["h"] = on_period_end
        return updater

    def _make_state(self, iteration=0, total_iterations=3):
        state = UpdateThreadState.new({"h": None}, 300.0, "test_entity_type", batch=True)
        state.iteration = iteration
        state.total_iterations = total_iterations
        return state

    def test_hook_receives_batch_filter_and_state_advances(self):
        updater = self._make_updater()
        state = self._make_state(iteration=1)
        batches = []

        def hook(entity_type, batch):
            batches.append((entity_type, batch))
            return {"records": 4}

        updater._process_batch_hooks("test_entity_type", {"h": hook}, state)

        [(entity_type, batch)] = batches
        self.assertEqual(entity_type, "test_entity_type")
        self.assertEqual(batch.query_filter, {"#hash": {"$mod": [3, 1]}})
        self.assertEqual((batch.iteration, batch.total_iterations), (1, 3))
        self.assertEqual(batch.period_start, state.t_created)
        self.assertEqual(state.iteration, 2)
        self.assertEqual(state.metrics, {"h": {"records": 4}})
        updater.cache.upsert.assert_called_once_with(state)

    def test_metrics_are_summed_and_passed_to_period_end(self):
        periods = []
        updater = self._make_updater(on_period_end=lambda etype, p: periods.append((etype, p)))
        state = self._make_state()
        period_start = state.t_created
        records = iter([2, 3, 5])

        def hook(entity_type, batch):
            return {"batches": 1, "records": next(records)}

        for _ in range(3):
            updater._process_batch_hooks("test_entity_type", {"h": hook}, state)

        [(entity_type, period)] = periods
        self.assertEqual(entity_type, "test_entity_type")
        self.assertEqual(period.period_start, period_start)
        self.assertEqual(period.metrics, {"batches": 3, "records": 10})
        self.assertGreaterEqual(period.runtime_secs, period.slowest_batch_secs)
        # The next period starts from zero
        self.assertEqual((state.iteration, state.metrics, state.finished), (0, {}, False))
        self.assertGreaterEqual(state.t_created, period_start)

    def test_batch_repeated_after_restart_is_counted_once(self):
        periods = []
        updater = self._make_updater(on_period_end=lambda etype, p: periods.append(p))
        state = self._make_state()

        def hook(entity_type, batch):
            return {"batches": 1, "records": 1}

        for _ in range(2):
            updater._process_batch_hooks("test_entity_type", {"h": hook}, state)
        saved = state.model_dump()

        # The worker stops after the last batch, before its progress is saved
        updater.cache.upsert.side_effect = SystemExit
        with self.assertRaises(SystemExit):
            updater._process_batch_hooks("test_entity_type", {"h": hook}, state)

        # After the restart, the last batch is repeated from the saved state
        updater.cache.upsert.side_effect = None
        restored = UpdateThreadState.model_validate(saved)
        updater._process_batch_hooks("test_entity_type", {"h": hook}, restored)

        self.assertEqual(len(periods), 2)
        self.assertEqual(periods[0].period_start, periods[1].period_start)
        self.assertEqual(periods[0].metrics, {"batches": 3, "records": 3})
        self.assertEqual(periods[1].metrics, periods[0].metrics)

    def test_failed_save_of_finished_period_is_retried_before_next_batch(self):
        periods = []
        updater = self._make_updater(on_period_end=lambda etype, p: periods.append(p))
        state = self._make_state(iteration=2)
        period_start = state.t_created
        batches = []

        def hook(entity_type, batch):
            batches.append(batch.iteration)
            return {"batches": 1}

        updater.cache.upsert.side_effect = RuntimeError("save failed")
        with self.assertRaises(RuntimeError):
            updater._process_batch_hooks("test_entity_type", {"h": hook}, state)
        self.assertTrue(state.finished)

        # The next invocation only saves the finished period, no batch is processed
        saved = []
        updater.cache.upsert.side_effect = lambda s: saved.append(s.model_dump())
        updater._process_batch_hooks("test_entity_type", {"h": hook}, state)
        self.assertEqual(batches, [2])
        self.assertEqual(len(periods), 1)
        self.assertEqual(len(saved), 1)
        self.assertEqual((saved[0]["t_created"], saved[0]["finished"]), (period_start, True))
        self.assertEqual((state.iteration, state.finished), (0, False))

        # The next period then starts with its first batch
        updater._process_batch_hooks("test_entity_type", {"h": hook}, state)
        self.assertEqual(batches, [2, 0])

    def test_failing_hook_does_not_stop_the_thread(self):
        updater = self._make_updater()
        state = self._make_state()

        def failing_hook(entity_type, batch):
            raise RuntimeError("boom")

        updater._process_batch_hooks("test_entity_type", {"h": failing_hook}, state)

        self.assertEqual(state.iteration, 1)
        self.assertEqual(state.metrics, {})
        updater.elog.log.assert_called_once_with("module_error")

    def test_period_start_survives_storage_precision(self):
        state = self._make_state()
        self.assertEqual(state.t_created.microsecond % 1000, 0)
        state.reset()
        self.assertEqual(state.t_created.microsecond % 1000, 0)


class TestUpdaterCacheUpsert(unittest.TestCase):
    def _upsert_filter(self, state):
        collection = MagicMock()
        UpdaterCache(collection).upsert(state)
        return collection.update_one.call_args.args[0]

    def test_record_thread_matches_states_saved_without_batch_field(self):
        state = UpdateThreadState.new({"h": None}, 300.0, "test_entity_type")
        self.assertEqual(self._upsert_filter(state)["batch"], {"$ne": True})

    def test_batch_thread_matches_only_batch_states(self):
        state = UpdateThreadState.new({"h": None}, 300.0, "test_entity_type", batch=True)
        self.assertIs(self._upsert_filter(state)["batch"], True)


if __name__ == "__main__":
    unittest.main()
