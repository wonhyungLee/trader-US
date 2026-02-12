from __future__ import annotations

import logging
import queue
import threading
import time
from typing import Any, Dict, Tuple

from src.utils.notifier import maybe_notify


class NotifyQueue:
    """Background notification sender.

    - Prevents unbounded thread creation from the web server.
    - Applies a small global throttle to avoid Discord 429 bursts.
    """

    def __init__(self, maxsize: int = 500, min_interval_sec: float = 0.35):
        self._q: queue.Queue[Tuple[Dict[str, Any], str]] = queue.Queue(maxsize=maxsize)
        self._min_interval = max(0.0, float(min_interval_sec))
        self._lock = threading.Lock()
        self._started = False
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            self._started = True
            self._thread.start()

    def send(self, settings: Dict[str, Any], message: str) -> None:
        if not message:
            return
        self.start()
        try:
            self._q.put_nowait((settings, message))
        except queue.Full:
            logging.warning("notify queue full; dropping message: %s", message[:120])

    def _run(self) -> None:
        last_ts = 0.0
        while True:
            settings, message = self._q.get()
            try:
                wait = self._min_interval - (time.time() - last_ts)
                if wait > 0:
                    time.sleep(wait)
                maybe_notify(settings, message)
            except Exception:
                logging.exception("notify send failed")
            finally:
                last_ts = time.time()
                self._q.task_done()


NOTIFY_QUEUE = NotifyQueue()


def notify_async(settings: Dict[str, Any], message: str) -> None:
    NOTIFY_QUEUE.send(settings, message)
