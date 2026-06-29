import logging
import threading
import time
import unittest
from contextlib import ExitStack
from unittest.mock import Mock, patch

from redis.exceptions import ConnectionError as RedisConnectionError

from dp3 import worker
from dp3.common.config import MissingConfigError
from dp3.task_processing.task_distributor import TaskDistributor


class TestWorkerLifecycle(unittest.TestCase):
    def test_worker_main_returns_failure_on_redis_startup_error(self):
        original_thread_name = threading.current_thread().name
        try:
            with (
                patch("dp3.worker.logging.shutdown"),
                patch(
                    "dp3.worker.EventCountLogger",
                    side_effect=RedisConnectionError("mock redis failure"),
                ),
            ):
                exit_code = worker.main("test", "tests/test_config", 0, False)
        finally:
            threading.current_thread().name = original_thread_name

        self.assertEqual(exit_code, 1)

    def test_worker_main_returns_configuration_error_on_missing_required_config(self):
        original_thread_name = threading.current_thread().name
        try:
            with (
                patch("dp3.worker.logging.shutdown"),
                patch(
                    "dp3.worker.read_config_dir",
                    side_effect=MissingConfigError("missing processing_core"),
                ),
            ):
                exit_code = worker.main("test", "tests/test_config", 0, False)
        finally:
            threading.current_thread().name = original_thread_name

        self.assertEqual(exit_code, 2)

    def test_worker_main_returns_configuration_error_on_missing_modules_dir(self):
        db = Mock()
        scheduler = Mock()
        ecl = Mock()
        ecl.get_group.return_value = None
        control = Mock()
        control.control_queue = Mock(watchdog=lambda: None)

        original_thread_name = threading.current_thread().name
        try:
            with ExitStack() as stack:
                stack.enter_context(patch("dp3.worker.signal.signal"))
                stack.enter_context(patch("dp3.worker.EventCountLogger", return_value=ecl))
                stack.enter_context(patch("dp3.worker.EntityDatabase", return_value=db))
                stack.enter_context(patch("dp3.worker.scheduler.Scheduler", return_value=scheduler))
                stack.enter_context(patch("dp3.worker.TaskExecutor", return_value=Mock()))
                stack.enter_context(patch("dp3.worker.TaskQueueWriter", return_value=Mock()))
                stack.enter_context(patch("dp3.worker.SnapShooter", return_value=Mock()))
                stack.enter_context(patch("dp3.worker.Updater", return_value=Mock()))
                stack.enter_context(patch("dp3.worker.CallbackRegistrar", return_value=Mock()))
                stack.enter_context(patch("dp3.worker.LinkManager"))
                stack.enter_context(patch("dp3.worker.HistoryManager"))
                stack.enter_context(patch("dp3.worker.Telemetry"))
                stack.enter_context(patch("dp3.worker.GarbageCollector"))
                stack.enter_context(patch("dp3.worker.Control", return_value=control))
                stack.enter_context(patch("dp3.worker.logging.shutdown"))
                stack.enter_context(
                    patch("dp3.worker.os.scandir", side_effect=FileNotFoundError("modules_dir"))
                )

                exit_code = worker.main("test", "tests/test_config", 0, False)
        finally:
            threading.current_thread().name = original_thread_name

        self.assertEqual(exit_code, 2)

    def test_task_distributor_forces_failure_exit_when_reader_stop_blocks(self):
        class ExitCalled(Exception):
            pass

        stop_started = threading.Event()

        def block_stop(timeout=None):
            stop_started.set()
            time.sleep(0.5)
            return True

        distributor = TaskDistributor.__new__(TaskDistributor)
        distributor.log = logging.getLogger("TaskDistributorTest")
        distributor.process_index = 0
        distributor.running = True
        distributor._worker_threads = []
        distributor._task_queue_reader = Mock()
        distributor._task_queue_reader.stop.side_effect = block_stop
        distributor._task_queue_writer = Mock()

        start_ts = time.monotonic()
        with (
            patch("dp3.task_processing.task_distributor.SHUTDOWN_TIME", 0.05),
            patch.object(distributor, "_dump_thread_stacks") as dump_thread_stacks,
            patch("dp3.task_processing.task_distributor.logging.shutdown"),
            patch(
                "dp3.task_processing.task_distributor.os._exit",
                side_effect=ExitCalled,
            ) as os_exit,
            self.assertRaises(ExitCalled),
        ):
            distributor.stop()
        elapsed = time.monotonic() - start_ts

        self.assertTrue(stop_started.is_set())
        self.assertLess(elapsed, 0.3)
        distributor._task_queue_reader.stop.assert_called_once()
        distributor._task_queue_reader.disconnect.assert_called_once()
        distributor._task_queue_writer.disconnect.assert_called_once()
        dump_thread_stacks.assert_called_once()
        os_exit.assert_called_once_with(1)


if __name__ == "__main__":
    unittest.main()
