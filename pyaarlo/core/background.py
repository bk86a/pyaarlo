import asyncio
import threading
import time
import traceback
from typing import Union, Callable, Dict, Any, Optional

from .logger import ArloLogger


class ArloBackground:
    """An asyncio-based background worker that supports both sync and async callbacks.

    This replaces the previous threading-based ArloBackgroundWorker. It allows for
    gradual migration of the codebase to asyncio while maintaining compatibility
    with existing synchronous code.
    """

    def __init__(self, log: ArloLogger):
        self._log: ArloLogger = log
        self._tasks: Dict[str, Union[asyncio.Task, asyncio.Future]] = {}
        self._counter: int = 0
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._started = threading.Event()

        # Try to get the running loop if it exists (e.g. if we're in an async context already)
        try:
            self._loop = asyncio.get_running_loop()
            self._started.set()
        except RuntimeError:
            # Otherwise, start a dedicated loop in a background thread to bridge sync code
            self._thread = threading.Thread(target=self._run_loop, name="ArloBackgroundLoop", daemon=True)
            self._thread.start()
            self._started.wait(timeout=5)

        self._log.debug("background: created (asyncio-based)")

    def _run_loop(self):
        """Dedicated thread for running the asyncio event loop."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._started.set()
        self._loop.run_forever()

    def _next_id(self) -> str:
        self._counter += 1
        return str(self._counter) + ":" + str(time.monotonic())

    async def _execute_job(self, job_id: str, cb: Callable, args: Dict[str, Any]):
        """Wraps the job execution to handle errors and cleanup."""
        try:
            if asyncio.iscoroutinefunction(cb):
                await cb(**args)
            else:
                # Run sync callbacks in the default executor (thread pool)
                await self._loop.run_in_executor(None, lambda: cb(**args))
        except Exception as e:
            self._log.error(
                f"background: job-error={type(e).__name__}\n{traceback.format_exc()}"
            )
        finally:
            self._tasks.pop(job_id, None)

    async def _execute_delayed_job(self, job_id: str, seconds: float, cb: Callable, args: Dict[str, Any]):
        """Wait for a specified delay before executing the job."""
        await asyncio.sleep(seconds)
        await self._execute_job(job_id, cb, args)

    async def _execute_periodic_job(self, job_id: str, seconds: float, cb: Callable, args: Dict[str, Any]):
        """Execute the job periodically."""
        while True:
            await asyncio.sleep(seconds)
            # We don't want the periodic job to pop itself from self._tasks until cancelled
            try:
                if asyncio.iscoroutinefunction(cb):
                    await cb(**args)
                else:
                    await self._loop.run_in_executor(None, lambda: cb(**args))
            except Exception as e:
                self._log.error(
                    f"background: periodic-job-error={type(e).__name__}\n{traceback.format_exc()}"
                )

    def _submit(self, coro) -> Union[asyncio.Task, asyncio.Future]:
        """Safely submit a coroutine to the event loop from any thread."""
        try:
            # If we are in the thread running the loop, we can use create_task
            if asyncio.get_running_loop() is self._loop:
                return asyncio.create_task(coro)
        except RuntimeError:
            # No loop running in this thread, or it's a different loop
            pass
        
        # Otherwise, we must use run_coroutine_threadsafe
        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    def _run(self, bg_cb, prio, **kwargs) -> str:
        # Priority is currently ignored in this implementation
        job_id = self._next_id()
        self._tasks[job_id] = self._submit(self._execute_job(job_id, bg_cb, kwargs))
        return job_id

    def run_high(self, bg_cb, **kwargs):
        return self._run(bg_cb, 10, **kwargs)

    def run(self, bg_cb, **kwargs):
        return self._run(bg_cb, 40, **kwargs)

    def run_low(self, bg_cb, **kwargs):
        return self._run(bg_cb, 99, **kwargs)

    def _run_in(self, bg_cb, prio, seconds, **kwargs) -> str:
        job_id = self._next_id()
        self._tasks[job_id] = self._submit(self._execute_delayed_job(job_id, seconds, bg_cb, kwargs))
        return job_id

    def run_high_in(self, bg_cb, seconds, **kwargs):
        return self._run_in(bg_cb, 10, seconds, **kwargs)

    def run_in(self, bg_cb, seconds, **kwargs):
        return self._run_in(bg_cb, 40, seconds, **kwargs)

    def run_low_in(self, bg_cb, seconds, **kwargs):
        return self._run_in(bg_cb, 99, seconds, **kwargs)

    def _run_every(self, bg_cb, prio, seconds, **kwargs) -> str:
        job_id = self._next_id()
        self._tasks[job_id] = self._submit(self._execute_periodic_job(job_id, seconds, bg_cb, kwargs))
        return job_id

    def run_high_every(self, bg_cb, seconds, **kwargs):
        return self._run_every(bg_cb, 10, seconds, **kwargs)

    def run_every(self, bg_cb, seconds, **kwargs) -> str:
        return self._run_every(bg_cb, 40, seconds, **kwargs)

    def run_low_every(self, bg_cb, seconds, **kwargs):
        return self._run_every(bg_cb, 99, seconds, **kwargs)

    def cancel(self, to_delete: str):
        if to_delete is not None and to_delete in self._tasks:
            task = self._tasks.pop(to_delete)
            task.cancel()
            return True
        return False

    def stop(self):
        """Stop the background worker and all pending tasks."""
        for job_id in list(self._tasks.keys()):
            self.cancel(job_id)

        if self._thread and self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=10)


