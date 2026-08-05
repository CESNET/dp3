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
from dp3.task_processing.task_queue import TaskQueueReader


def make_task_queue_reader(**overrides):
    reader = TaskQueueReader.__new__(TaskQueueReader)
    defaults = {
        "log": logging.getLogger("TaskQueueReaderTest"),
        "worker_index": 0,
        "running": False,
        "_processing_thread": None,
        "_consuming_thread": None,
        "_watchdog_recovery_thread": None,
        "_stopping": False,
        "connection": None,
        "channel": None,
        "cache": [],
        "cache_pri": [],
        "cache_full": threading.Event(),
        "connect": Mock(),
        "start": Mock(),
    }
    defaults.update(overrides)
    for name, value in defaults.items():
        setattr(reader, name, value)
    return reader


class TestWorkerLifecycle(unittest.TestCase):
    def test_shutdown_callback_is_bounded(self):
        release = threading.Event()
        started = time.monotonic()

        completed, error = worker._run_shutdown_callback(release.wait, 0.02, "BlockingShutdownTest")

        self.assertFalse(completed)
        self.assertIsNone(error)
        self.assertLess(time.monotonic() - started, 0.2)
        release.set()

    def test_hook_telemetry_flush_uses_remaining_shutdown_deadline(self):
        release = threading.Event()
        hook_elog = Mock(sync=release.wait)
        started = time.monotonic()

        flushed = worker._flush_hook_telemetry(
            hook_elog, time.monotonic() + 0.02, logging.getLogger(self.id())
        )

        self.assertFalse(flushed)
        self.assertLess(time.monotonic() - started, 0.2)
        release.set()

    def test_hook_telemetry_flush_skips_exhausted_deadline(self):
        hook_elog = Mock()

        flushed = worker._flush_hook_telemetry(
            hook_elog, time.monotonic() - 1, logging.getLogger(self.id())
        )

        self.assertTrue(flushed)
        hook_elog.sync.assert_not_called()

    def test_hook_telemetry_flush_reports_failure(self):
        hook_elog = Mock()
        hook_elog.sync.side_effect = RuntimeError("flush failed")

        flushed = worker._flush_hook_telemetry(
            hook_elog, time.monotonic() + 1, logging.getLogger(self.id())
        )

        self.assertFalse(flushed)

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

    def test_worker_main_stops_core_module_when_start_fails_after_partial_start(self):
        class FailingTaskDistributor:
            def __init__(self, task_executor, platform_config, registrar, daemon_stop_lock):
                self.stopped = False
                task_distributors.append(self)

            def start(self):
                raise RuntimeError("partial startup failure")

            def stop(self):
                self.stopped = True

        task_distributors = []
        db = Mock()
        scheduler = Mock()
        ecl = Mock()
        ecl.get_group.return_value = None
        control = Mock()
        control.control_queue = Mock(watchdog=lambda: None)

        original_thread_name = threading.current_thread().name
        try:
            with ExitStack() as stack:
                # Replace external services and long-running components so the
                # test reaches core module startup without Redis, MongoDB, or RabbitMQ.
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
                stack.enter_context(patch("dp3.worker.load_modules", return_value={}))
                stack.enter_context(patch("dp3.worker.Control", return_value=control))
                stack.enter_context(patch("dp3.worker.TaskDistributor", FailingTaskDistributor))
                stack.enter_context(patch("dp3.worker.logging.shutdown"))

                exit_code = worker.main("test", "tests/test_config", 0, False)
        finally:
            threading.current_thread().name = original_thread_name

        self.assertEqual(exit_code, 1)
        self.assertEqual(len(task_distributors), 1)
        self.assertTrue(task_distributors[0].stopped)

    def test_task_queue_reader_watchdog_does_not_block_on_channel_close(self):
        close_started = threading.Event()
        release_close = threading.Event()

        class BlockingChannel:
            def close(self):
                close_started.set()
                release_close.wait()

        reader = make_task_queue_reader(channel=BlockingChannel())

        watchdog_thread = threading.Thread(target=reader.watchdog, daemon=True)
        watchdog_thread.start()
        self.assertTrue(close_started.wait(timeout=0.2))
        try:
            watchdog_thread.join(timeout=0.2)
            self.assertFalse(watchdog_thread.is_alive())
        finally:
            release_close.set()
            watchdog_thread.join(timeout=1)
            reader._watchdog_recovery_thread.join(timeout=1)

        reader.connect.assert_called_once()
        reader.start.assert_called_once()

    def test_main_task_reader_watchdog_recovery_does_not_request_worker_shutdown(self):
        close_started = threading.Event()
        release_close = threading.Event()

        class BlockingChannel:
            def close(self):
                close_started.set()
                release_close.wait()

        class Registrar:
            def scheduler_register(self, callback, **kwargs):
                pass

        def config_get(key, default=None):
            values = {
                "processing_core.msg_broker": {},
                "db_entities": {"entity": Mock()},
                "processing_core.worker_threads": 1,
            }
            return values.get(key, default)

        daemon_stop_lock = threading.Lock()
        daemon_stop_lock.acquire()
        platform_config = Mock()
        platform_config.process_index = 0
        platform_config.num_processes = 1
        platform_config.model_spec = Mock()
        platform_config.app_name = "test"
        platform_config.config.get.side_effect = config_get
        distributor = TaskDistributor(Mock(), platform_config, Registrar(), daemon_stop_lock)
        reader = distributor._task_queue_reader
        reader._processing_thread = None
        reader._consuming_thread = None
        reader.connection = None
        reader.channel = BlockingChannel()
        reader.cache = []
        reader.cache_pri = []
        reader.connect = Mock()
        reader.start = Mock()

        watchdog_thread = threading.Thread(target=reader.watchdog, daemon=True)
        watchdog_thread.start()
        self.assertTrue(close_started.wait(timeout=0.2))
        try:
            watchdog_thread.join(timeout=0.2)
            self.assertFalse(watchdog_thread.is_alive())
            self.assertFalse(
                daemon_stop_lock.acquire(timeout=0.1),
                "RabbitMQ reader watchdog recovery should keep retrying instead of "
                "requesting worker shutdown.",
            )
        finally:
            release_close.set()
            watchdog_thread.join(timeout=1)
            reader._watchdog_recovery_thread.join(timeout=1)

    def test_stopped_task_queue_reader_recovery_cannot_mutate_new_reader_state(self):
        close_started = threading.Event()
        release_close = threading.Event()

        class BlockingChannel:
            def close(self):
                close_started.set()
                release_close.wait()

        new_channel = Mock(name="new_channel")
        reader = make_task_queue_reader(
            channel=BlockingChannel(),
            cache=["old-normal"],
            cache_pri=["old-priority"],
        )

        reader.watchdog()
        self.assertTrue(close_started.wait(timeout=0.2))
        self.assertFalse(reader.stop(timeout=0.01))
        reader.channel = new_channel
        reader.cache = ["new-normal"]
        reader.cache_pri = ["new-priority"]

        release_close.set()
        reader._watchdog_recovery_thread.join(timeout=1)

        self.assertIs(
            reader.channel,
            new_channel,
            "A stale recovery thread must not clear a channel installed later.",
        )
        self.assertEqual(reader.cache, ["new-normal"])
        self.assertEqual(reader.cache_pri, ["new-priority"])

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

    def test_task_distributor_stop_tolerates_not_started_reader(self):
        class ExitCalled(Exception):
            pass

        distributor = TaskDistributor.__new__(TaskDistributor)
        distributor.log = logging.getLogger("TaskDistributorTest")
        distributor.process_index = 0
        distributor.running = True
        distributor._worker_threads = []
        distributor._task_queue_reader = Mock()
        distributor._task_queue_reader.running = False
        distributor._task_queue_reader.stop.return_value = True
        distributor._task_queue_writer = Mock()

        with (
            patch("dp3.task_processing.task_distributor.SHUTDOWN_TIME", 0.05),
            patch.object(distributor, "_dump_thread_stacks"),
            patch("dp3.task_processing.task_distributor.logging.shutdown"),
            patch(
                "dp3.task_processing.task_distributor.os._exit",
                side_effect=ExitCalled,
            ) as os_exit,
        ):
            distributor.stop()

        self.assertFalse(distributor.running)
        distributor._task_queue_reader.stop.assert_called_once()
        distributor._task_queue_reader.disconnect.assert_called_once()
        distributor._task_queue_writer.disconnect.assert_called_once()
        os_exit.assert_not_called()

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
