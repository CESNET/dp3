import copy
import unittest
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from dp3.history_management.history_manager import aggregate_multivalue_dp_history_on_equal

T0 = datetime(2026, 10, 27, tzinfo=UTC)
VALIDITY = timedelta(hours=8)


def hashable_spec(hashable: bool):
    return SimpleNamespace(
        history_params=SimpleNamespace(pre_validity=VALIDITY, post_validity=VALIDITY),
        data_type=SimpleNamespace(hashable=hashable),
    )


class MultiValueAggregationOrder(unittest.TestCase):
    """Aggregated multi-value histories keep a stable `(t1, t2)` order between runs."""

    VARIANTS = {
        "hashable": (True, lambda port: port),
        "unhashable": (False, lambda port: {"port": port}),
    }

    def dp(self, value, hour: float, end_hour: float | None = None) -> dict:
        end_hour = hour if end_hour is None else end_hour
        return {
            "t1": T0 + timedelta(hours=hour),
            "t2": T0 + timedelta(hours=end_hour),
            "v": value,
            "c": 1.0,
        }

    def aggregate(self, hashable: bool, history: list[dict]):
        # Aggregation modifies datapoints in place, work on a copy
        return aggregate_multivalue_dp_history_on_equal(
            copy.deepcopy(history), hashable_spec(hashable)
        )

    def stored_history(self, value) -> list[dict]:
        """Already aggregated history: one recurring value and many one-off values."""
        history = [self.dp(value(443), 0)]
        history += [self.dp(value(50000 + i), 1 + i * 0.1) for i in range(20)]
        return history

    def assert_time_ordered(self, history: list[dict]):
        keys = [(dp["t1"], dp["t2"]) for dp in history]
        self.assertEqual(keys, sorted(keys))

    def test_result_is_time_ordered(self):
        for name, (hashable, value) in self.VARIANTS.items():
            with self.subTest(name):
                # Recurring value without overlap finalizes its first datapoint mid-history
                history = self.stored_history(value)
                history += [self.dp(value(443), 30), self.dp(value(50003), 31)]
                aggregated, _ = self.aggregate(hashable, history)
                self.assert_time_ordered(aggregated)

    def test_stored_history_is_stable(self):
        for name, (hashable, value) in self.VARIANTS.items():
            with self.subTest(name):
                history = self.stored_history(value)
                aggregated, changed = self.aggregate(hashable, history)
                self.assertFalse(changed)
                self.assertEqual(aggregated, history)

                # Aggregation output aggregated again stays as it is
                _, changed_again = self.aggregate(hashable, aggregated)
                self.assertFalse(changed_again)

    def test_recurring_value_without_overlap_needs_no_rewrite(self):
        """A value re-appearing after its validity used to reorder the whole history."""
        for name, (hashable, value) in self.VARIANTS.items():
            with self.subTest(name):
                history = self.stored_history(value) + [self.dp(value(443), 30)]
                aggregated, changed = self.aggregate(hashable, history)
                self.assertFalse(changed)
                self.assertEqual(aggregated, history)

    def test_merge_changes_only_merged_position_and_tail(self):
        for name, (hashable, value) in self.VARIANTS.items():
            with self.subTest(name):
                stored = self.stored_history(value)
                merged_idx = 4
                new_dp = self.dp(stored[merged_idx]["v"], 5)  # overlaps the stored datapoint
                aggregated, changed = self.aggregate(hashable, stored + [new_dp])

                self.assertTrue(changed)
                self.assertEqual(len(aggregated), len(stored))
                for i, (before, after) in enumerate(zip(stored, aggregated, strict=True)):
                    if i == merged_idx:
                        self.assertEqual(after["v"], before["v"])
                        self.assertEqual(after["t1"], before["t1"])
                        self.assertEqual(after["t2"], new_dp["t2"])
                    else:
                        self.assertEqual(after, before, f"position {i} changed")

    def test_late_unsorted_input_is_normalized(self):
        for name, (hashable, value) in self.VARIANTS.items():
            with self.subTest(name):
                history = self.stored_history(value)
                late = self.dp(value(60000), 0.5)  # arrived late, older than the tail
                aggregated, changed = self.aggregate(hashable, history + [late])
                self.assertTrue(changed)
                self.assert_time_ordered(aggregated)
                self.assertEqual(len(aggregated), len(history) + 1)

    def test_merge_within_equal_t1_group_keeps_positions(self):
        """Values observed together share `t1`, extending one `t2` must not move it."""
        for name, (hashable, value) in self.VARIANTS.items():
            with self.subTest(name):
                stored = [self.dp(value(50000 + i), 0) for i in range(20)]
                new_dp = self.dp(value(50000), 1)  # overlaps the first datapoint
                aggregated, changed = self.aggregate(hashable, stored + [new_dp])

                self.assertTrue(changed)
                self.assertEqual(aggregated[0]["v"], stored[0]["v"])
                self.assertEqual(aggregated[0]["t2"], new_dp["t2"])
                self.assertEqual(aggregated[1:], stored[1:])

    def test_finalization_within_equal_t1_group_needs_no_rewrite(self):
        """Finalizing a later datapoint of an equal `t1` group must not reorder the group."""
        for name, (hashable, value) in self.VARIANTS.items():
            with self.subTest(name):
                stored = [self.dp(value(80), 0), self.dp(value(443), 0)]
                history = stored + [self.dp(value(443), 30)]  # finalizes the stored 443
                aggregated, changed = self.aggregate(hashable, history)

                self.assertFalse(changed)
                self.assertEqual(aggregated, history)


if __name__ == "__main__":
    unittest.main()
