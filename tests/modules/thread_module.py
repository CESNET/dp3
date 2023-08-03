import logging
import threading
import time

from dp3.common.base_module import BaseModule


class MyModule(BaseModule):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._thread = None
        self._stop_event = threading.Event()
        self.log = logging.getLogger("MyModule")

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        self._thread.join()

    def _run(self):
        while not self._stop_event.is_set():
            self.log.info("Hello world!")
            time.sleep(1)
