import logging
import queue
import threading
import time
import unittest
from contextlib import ExitStack, suppress
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

    def test_worker_main_forces_failure_exit_when_core_module_stop_blocks(self):
        stop_started = threading.Event()
        release_stop = threading.Event()

        class ExitCalled(Exception):
            pass

        class FakeControl:
            def __init__(self, platform_config):
                self.control_queue = Mock(watchdog=lambda: None)

            def set_action_handler(self, action, handler):
                pass

            def start(self):
                pass

            def stop(self):
                stop_started.set()
                release_stop.wait()

        class FakeTaskDistributor:
            def __init__(self, task_executor, platform_config, registrar, daemon_stop_lock):
                self.daemon_stop_lock = daemon_stop_lock

            def start(self):
                self.daemon_stop_lock.release()

            def stop(self):
                pass

        db = Mock()
        scheduler = Mock()
        ecl = Mock()
        ecl.get_group.return_value = None

        def run_worker():
            with ExitStack() as stack:
                # Keep the test fast and independent of process-global signal handlers.
                stack.enter_context(patch("dp3.worker.WORKER_SHUTDOWN_TIME", 0.05, create=True))
                stack.enter_context(patch("dp3.worker.signal.signal"))

                # Replace external services and infrastructure-heavy components so
                # worker.main() can reach its shutdown path without Redis, MongoDB,
                # RabbitMQ, scheduler threads, or loaded DP3 modules.
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
                stack.enter_context(patch("dp3.worker.load_modules", return_value={}))

                # FakeTaskDistributor releases daemon_stop_lock so worker.main()
                # enters cleanup; FakeControl.stop() then blocks to verify the
                # global cleanup timeout protects every core module stop.
                stack.enter_context(patch("dp3.worker.Control", FakeControl))
                stack.enter_context(patch("dp3.worker.TaskDistributor", FakeTaskDistributor))

                # Prevent the test process from exiting or printing full thread
                # dumps when the forced-shutdown path is exercised.
                stack.enter_context(patch("dp3.worker.logging.shutdown"))
                stack.enter_context(patch("dp3.worker.faulthandler.dump_traceback"))
                os_exit = stack.enter_context(patch("dp3.worker.os._exit", side_effect=ExitCalled))
                with suppress(ExitCalled):
                    worker.main("test", "tests/test_config", 0, False)
                return os_exit.called

        worker_thread = threading.Thread(target=run_worker, daemon=True)
        worker_thread.start()
        try:
            self.assertTrue(stop_started.wait(timeout=1))
            worker_thread.join(timeout=0.2)
            self.assertFalse(worker_thread.is_alive())
        finally:
            release_stop.set()
            worker_thread.join(timeout=1)

    def test_task_distributor_forces_exit_before_blocking_disconnect_after_reader_timeout(self):
        stop_started = threading.Event()
        disconnect_started = threading.Event()
        release_disconnect = threading.Event()

        def block_stop(timeout=None):
            stop_started.set()
            time.sleep(0.5)
            return True

        def block_disconnect():
            disconnect_started.set()
            release_disconnect.wait()

        distributor = TaskDistributor.__new__(TaskDistributor)
        distributor.log = logging.getLogger("TaskDistributorTest")
        distributor.process_index = 0
        distributor.running = True
        distributor._worker_threads = []
        distributor._task_queue_reader = Mock()
        distributor._task_queue_reader.stop.side_effect = block_stop
        distributor._task_queue_reader.disconnect.side_effect = block_disconnect
        distributor._task_queue_writer = Mock()

        stop_error = []

        def run_stop():
            try:
                distributor.stop()
            except Exception as exc:
                stop_error.append(exc)

        class ExitCalled(Exception):
            pass

        with (
            patch("dp3.task_processing.task_distributor.SHUTDOWN_TIME", 0.05),
            patch.object(distributor, "_dump_thread_stacks"),
            patch("dp3.task_processing.task_distributor.logging.shutdown"),
            patch(
                "dp3.task_processing.task_distributor.os._exit",
                side_effect=ExitCalled,
            ) as os_exit,
        ):
            stop_thread = threading.Thread(target=run_stop, daemon=True)
            stop_thread.start()
            self.assertTrue(stop_started.wait(timeout=0.2))
            try:
                stop_thread.join(timeout=0.2)
                self.assertFalse(stop_thread.is_alive())
            finally:
                release_disconnect.set()
                stop_thread.join(timeout=1)

        self.assertFalse(disconnect_started.is_set())
        self.assertTrue(any(isinstance(exc, ExitCalled) for exc in stop_error))
        os_exit.assert_called_once_with(1)

    def test_task_distributor_keeps_workers_running_while_reader_shutdown_finishes(self):
        stop_started = threading.Event()
        release_stop = threading.Event()

        def block_stop(timeout=None):
            stop_started.set()
            release_stop.wait()
            return True

        distributor = TaskDistributor.__new__(TaskDistributor)
        distributor.log = logging.getLogger("TaskDistributorTest")
        distributor.process_index = 0
        distributor.running = True
        distributor._worker_threads = []
        distributor._task_queue_reader = Mock()
        distributor._task_queue_reader.stop.side_effect = block_stop
        distributor._task_queue_writer = Mock()

        stop_thread = threading.Thread(target=distributor.stop, daemon=True)
        with patch("dp3.task_processing.task_distributor.SHUTDOWN_TIME", 0.5):
            stop_thread.start()
            self.assertTrue(stop_started.wait(timeout=0.2))
            try:
                time.sleep(0.05)
                self.assertTrue(distributor.running)
            finally:
                release_stop.set()
                stop_thread.join(timeout=1)

        self.assertFalse(distributor.running)

    def test_task_distributor_does_not_block_distributing_tasks_after_shutdown_starts(self):
        distributor = TaskDistributor.__new__(TaskDistributor)
        distributor.num_threads = 1
        distributor._accepting_tasks = False
        local_queue = queue.Queue(maxsize=1)
        local_queue.put(("existing-id", Mock()))
        distributor._queues = [local_queue]
        task = Mock()
        task.routing_key.return_value = "entity"

        distribute_thread = threading.Thread(
            target=distributor._distribute_task,
            args=("msg-id", task),
            daemon=True,
        )
        try:
            distribute_thread.start()
            distribute_thread.join(timeout=0.2)
            self.assertFalse(distribute_thread.is_alive())
        finally:
            with suppress(queue.Empty):
                local_queue.get_nowait()
            distribute_thread.join(timeout=1)


if __name__ == "__main__":
    unittest.main()
