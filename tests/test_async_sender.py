"""Tests for _AsyncSender, _SyncSender, and _ForkSafeLock."""

import os
import threading

import pytest

import picotel
from picotel import (
    PicotelConfigError,
    Resource,
    Span,
    _AsyncSender,
    _ForkSafeLock,
    new_span_id,
    new_trace_id,
    now_ns,
)

# ---------------------------------------------------------------------------
# _AsyncSender unit tests
# ---------------------------------------------------------------------------


def test_submit_executes_callable():
    """Submitted callable runs in the background thread."""
    sender = _AsyncSender()
    done = threading.Event()
    sender.submit(done.set)
    assert done.wait(timeout=2), "callable was not executed"


def test_thread_is_daemon():
    """Worker thread must be a daemon so it doesn't block process exit."""
    sender = _AsyncSender()
    done = threading.Event()
    sender.submit(done.set)
    done.wait(timeout=2)
    assert sender._thread.daemon is True


def test_queue_full_returns_false():
    """submit() returns False when the internal queue is full."""
    started = threading.Event()
    release = threading.Event()
    sender = _AsyncSender(maxsize=1)

    def blocker():
        started.set()
        release.wait()

    sender.submit(blocker)
    assert started.wait(timeout=2), "worker did not dequeue the blocker"
    # Queue is empty (worker dequeued blocker), fill it
    assert sender.submit(lambda: None) is True
    # Queue is now full
    assert sender.submit(lambda: None) is False
    release.set()


def test_concurrent_submits_create_single_thread():
    """Multiple concurrent submits must not create duplicate threads."""
    sender = _AsyncSender()
    barrier = threading.Barrier(10)
    results = []

    def concurrent_submit():
        barrier.wait()
        sender.submit(lambda: None)
        results.append(sender._thread)

    threads = [threading.Thread(target=concurrent_submit) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)
    for t in threads:
        assert not t.is_alive(), "concurrent submit thread did not finish"

    assert len({id(t) for t in results}) == 1


def test_is_alive_false_before_first_submit():
    """is_alive() returns False before any work is submitted."""
    sender = _AsyncSender()
    assert sender.is_alive() is False


def test_is_alive_true_after_submit():
    """is_alive() returns True once the worker thread is running."""
    sender = _AsyncSender()
    done = threading.Event()
    sender.submit(done.set)
    done.wait(timeout=2)
    assert sender.is_alive() is True


def test_is_alive_false_after_pid_change():
    """is_alive() returns False when PID no longer matches (simulated fork)."""
    sender = _AsyncSender()
    done = threading.Event()
    sender.submit(done.set)
    done.wait(timeout=2)
    assert sender.is_alive() is True

    # Simulate fork by changing the stored PID
    sender._pid = -1
    assert sender.is_alive() is False


def test_submit_creates_new_thread_after_pid_change():
    """submit() creates a fresh thread/queue when PID mismatch is detected."""
    sender = _AsyncSender()
    done1 = threading.Event()
    sender.submit(done1.set)
    done1.wait(timeout=2)
    old_thread = sender._thread

    # Simulate fork by changing the stored PID
    sender._pid = -1

    done2 = threading.Event()
    sender.submit(done2.set)
    assert done2.wait(timeout=2), "new thread did not execute callable"
    assert sender._thread is not old_thread, "thread should have been recreated"


def test_config_error_in_worker_is_logged(picotel_caplog):
    """PicotelConfigError raised in a submitted callable is logged by the worker."""
    done = threading.Event()
    sender = _AsyncSender()

    def raise_config_error():
        raise picotel.PicotelConfigError("test config error")

    sender.submit(raise_config_error)
    sender.submit(done.set)
    done.wait(timeout=2)

    assert any("test config error" in r.message for r in picotel_caplog.records)


def test_error_in_callable_does_not_kill_worker():
    """An exception in one callable must not prevent subsequent work."""
    sender = _AsyncSender()
    done = threading.Event()

    sender.submit(lambda: 1 / 0)
    sender.submit(done.set)

    assert done.wait(timeout=2), "worker died after exception"


def test_queue_full_warned_logs_once_then_resets(picotel_caplog):
    """Queue-full error is logged once per episode; resets after success."""
    started = threading.Event()
    release = threading.Event()
    sender = _AsyncSender(maxsize=1)

    def blocker():
        started.set()
        release.wait()

    sender.submit(blocker)
    assert started.wait(timeout=2)

    # Fill the queue, then overflow twice — only one error message expected
    assert sender.submit(lambda: None) is True  # fills queue
    assert sender.submit(lambda: None) is False  # first drop
    assert sender.submit(lambda: None) is False  # second drop (no new log)

    full_messages = [r for r in picotel_caplog.records if "queue full" in r.message]
    assert len(full_messages) == 1

    # Unblock the worker so the queue drains, then retry until a submit
    # succeeds (which resets the _queue_full_warned guard).
    release.set()
    done = threading.Event()
    for _ in range(50):
        if sender.submit(done.set):
            break
        threading.Event().wait(0.05)
    assert done.wait(timeout=2), "worker did not resume"

    # Block and overflow again — should produce a second error message
    started2 = threading.Event()
    release2 = threading.Event()

    def blocker2():
        started2.set()
        release2.wait()

    sender.submit(blocker2)
    assert started2.wait(timeout=2)
    sender.submit(lambda: None)  # fill
    sender.submit(lambda: None)  # overflow

    full_messages = [r for r in picotel_caplog.records if "queue full" in r.message]
    assert len(full_messages) == 2
    release2.set()


# ---------------------------------------------------------------------------
# _AsyncSender circuit breaker
# ---------------------------------------------------------------------------


def _wait_for_worker(sender):
    """Block until the worker has processed all previously-submitted work."""
    done = threading.Event()
    assert sender.submit(done.set) is True
    assert done.wait(timeout=2), "worker did not reach sync point"


def test_async_sender_trips_after_consecutive_failures(picotel_caplog):
    """Circuit breaker trips after _MAX_CONSECUTIVE_ERRORS False returns."""
    sender = _AsyncSender()
    for _ in range(sender._MAX_CONSECUTIVE_ERRORS):
        assert sender.submit(lambda: False) is True
    # Give the worker a chance to process all queued failures.
    for _ in range(50):
        if sender._tripped:
            break
        threading.Event().wait(0.02)
    assert sender._tripped is True
    # submit() is now a no-op
    called = []
    assert sender.submit(lambda: called.append(1)) is False
    # Worker never sees the post-trip submit (it never entered the queue).
    assert called == []
    assert any(
        "further sends are disabled" in r.message for r in picotel_caplog.records
    )


def test_async_sender_config_error_trips_breaker(picotel_caplog):
    """PicotelConfigError raised by the callable counts as a persistent failure."""
    sender = _AsyncSender()

    def raise_config():
        raise picotel.PicotelConfigError("no endpoint")

    for _ in range(sender._MAX_CONSECUTIVE_ERRORS):
        assert sender.submit(raise_config) is True
    for _ in range(50):
        if sender._tripped:
            break
        threading.Event().wait(0.02)
    assert sender._tripped is True
    assert any(
        "further sends are disabled" in r.message for r in picotel_caplog.records
    )


def test_async_sender_other_exception_does_not_trip():
    """Non-persistent exceptions (e.g. ValueError) do not count toward the breaker."""
    sender = _AsyncSender()
    for _ in range(sender._MAX_CONSECUTIVE_ERRORS + 5):
        sender.submit(lambda: (_ for _ in ()).throw(ValueError("boom")))
    _wait_for_worker(sender)
    assert sender._tripped is False
    # A ValueError does not reset the counter either — the counter only
    # moves on persistent-failure paths — but it must not advance it.
    assert sender._consecutive_errors == 0


def test_async_sender_success_resets_error_count():
    """A successful send resets the consecutive-error counter."""
    sender = _AsyncSender()
    # Just below the threshold
    for _ in range(sender._MAX_CONSECUTIVE_ERRORS - 1):
        sender.submit(lambda: False)
    # Inject a success. _wait_for_worker's sentinel submit itself returns
    # None (truthy — not False), so this call also acts as the success.
    _wait_for_worker(sender)
    assert sender._consecutive_errors == 0
    assert sender._tripped is False
    # Another full run of failures must still not trip (N-1 failures).
    for _ in range(sender._MAX_CONSECUTIVE_ERRORS - 1):
        sender.submit(lambda: False)
    _wait_for_worker(sender)
    assert sender._tripped is False


def test_async_sender_is_alive_not_affected_by_trip():
    """is_alive() stays tied to the thread, not to _tripped.

    This is deliberate: binding is_alive() to _tripped would cause the
    double-checked is_alive() path in submit() to respawn the worker
    and replace the queue on every post-trip call.
    """
    sender = _AsyncSender()
    for _ in range(sender._MAX_CONSECUTIVE_ERRORS):
        sender.submit(lambda: False)
    for _ in range(50):
        if sender._tripped:
            break
        threading.Event().wait(0.02)
    assert sender._tripped is True
    # Thread is still running, so is_alive() is still True.
    assert sender.is_alive() is True


def test_async_sender_tripped_does_not_enqueue():
    """Post-trip, submit() must not put new items on the queue."""
    sender = _AsyncSender()
    for _ in range(sender._MAX_CONSECUTIVE_ERRORS):
        sender.submit(lambda: False)
    for _ in range(50):
        if sender._tripped:
            break
        threading.Event().wait(0.02)
    assert sender._tripped is True
    qsize_before = sender._queue.qsize()
    for _ in range(10):
        assert sender.submit(lambda: None) is False
    assert sender._queue.qsize() == qsize_before


# ---------------------------------------------------------------------------
# Integration: error resilience via async sender
# ---------------------------------------------------------------------------


def test_span_exit_error_does_not_crash_caller():
    """Errors during background send do not propagate to the caller."""
    picotel._sender = _AsyncSender()
    resource = Resource(attributes={"service.name": "test"})
    with Span(
        trace_id=new_trace_id(),
        span_id=new_span_id(),
        name="error-test",
        start_time_ns=now_ns(),
        resource=resource,
        endpoint="http://127.0.0.1:1",
    ):
        pass
    # Reaching here means the error was swallowed correctly


# ---------------------------------------------------------------------------
# _SyncSender unit tests
# ---------------------------------------------------------------------------


def test_sync_sender_executes_callable():
    """_SyncSender.submit() executes the callable immediately."""
    sender = picotel._SyncSender()
    result = []
    sender.submit(result.append, 42)
    assert result == [42]


def test_sync_sender_logs_config_error(picotel_caplog):
    """_SyncSender logs PicotelConfigError without raising."""
    sender = picotel._SyncSender()

    def raise_config():
        raise picotel.PicotelConfigError("sync config error")

    sender.submit(raise_config)
    assert any("sync config error" in r.message for r in picotel_caplog.records)


def test_sync_sender_logs_other_exceptions(picotel_caplog):
    """Non-config exceptions are logged but do not count toward the circuit breaker."""
    sender = picotel._SyncSender()
    for _ in range(sender._MAX_CONSECUTIVE_ERRORS + 5):
        assert sender.submit(lambda: 1 / 0) is True
    assert sender._tripped is False
    assert sender._consecutive_errors == 0
    assert any("Telemetry send error" in r.message for r in picotel_caplog.records)


# ---------------------------------------------------------------------------
# _SyncSender circuit breaker
# ---------------------------------------------------------------------------


def test_sync_sender_trips_after_consecutive_failures(picotel_caplog):
    """Circuit breaker trips after _MAX_CONSECUTIVE_ERRORS consecutive False returns."""
    sender = picotel._SyncSender()
    for _ in range(sender._MAX_CONSECUTIVE_ERRORS - 1):
        assert sender.submit(lambda: False) is True
    # The Nth failure trips the breaker
    assert sender.submit(lambda: False) is False
    assert sender._tripped is True
    assert any(
        "further sends are disabled" in r.message for r in picotel_caplog.records
    )


def test_sync_sender_success_resets_error_count():
    """A successful send resets the consecutive error counter."""
    sender = picotel._SyncSender()
    # Accumulate failures just below the threshold
    for _ in range(sender._MAX_CONSECUTIVE_ERRORS - 1):
        sender.submit(lambda: False)
    # One success resets the counter
    sender.submit(lambda: True)
    assert sender._consecutive_errors == 0
    assert sender._tripped is False
    # Need another full sequence to trip
    for _ in range(sender._MAX_CONSECUTIVE_ERRORS - 1):
        sender.submit(lambda: False)
    assert sender._tripped is False


def test_sync_sender_tripped_drops_all_work():
    """Once tripped, all subsequent submits return False without calling fn."""
    sender = picotel._SyncSender()
    for _ in range(sender._MAX_CONSECUTIVE_ERRORS):
        sender.submit(lambda: False)
    assert sender._tripped is True
    # fn should never be called once tripped
    called = []
    assert sender.submit(lambda: called.append(1)) is False
    assert called == []


def test_sync_sender_config_error_trips_breaker(picotel_caplog):
    """PicotelConfigError counts toward the circuit breaker (persistent failure)."""
    sender = picotel._SyncSender()

    def raise_config():
        raise picotel.PicotelConfigError("no endpoint")

    for _ in range(sender._MAX_CONSECUTIVE_ERRORS - 1):
        assert sender.submit(raise_config) is True
    assert sender.submit(raise_config) is False
    assert sender._tripped is True
    assert any(
        "further sends are disabled" in r.message for r in picotel_caplog.records
    )


def test_sync_sender_is_alive_false_when_tripped():
    """is_alive() returns False once the circuit breaker has tripped."""
    sender = picotel._SyncSender()
    assert sender.is_alive() is True
    for _ in range(sender._MAX_CONSECUTIVE_ERRORS):
        sender.submit(lambda: False)
    assert sender.is_alive() is False


# ---------------------------------------------------------------------------
# _AsyncSender fork recovery
# ---------------------------------------------------------------------------


def test_async_sender_replaces_queue_after_pid_change():
    """Queue is replaced after fork to avoid poisoned internal locks."""
    sender = _AsyncSender()
    done = threading.Event()
    sender.submit(done.set)
    done.wait(timeout=2)
    old_queue = sender._queue

    sender._pid = -1

    done2 = threading.Event()
    sender.submit(done2.set)
    assert done2.wait(timeout=2), "new worker did not execute callable"
    assert sender._queue is not old_queue


# ---------------------------------------------------------------------------
# _ForkSafeLock unit tests
# ---------------------------------------------------------------------------


def test_fork_safe_lock_normal_acquire_release():
    """Basic acquire/release works without fork."""
    lock = _ForkSafeLock()
    with lock:
        pass  # should not deadlock or raise


def test_fork_safe_lock_replaces_lock_after_pid_change():
    """_ForkSafeLock replaces its internal lock when PID changes (fork)."""
    lock = _ForkSafeLock()
    old_lock = lock._lock
    lock._pid = -1
    with lock:
        pass
    assert lock._lock is not old_lock
    assert lock._pid == os.getpid()


def test_fork_safe_lock_recovers_from_poisoned_probe():
    """When probe is stuck (poisoned by fork), timeout triggers replacement."""
    lock = _ForkSafeLock(timeout=0.1)
    lock._pid = -1
    # Simulate poisoned probe: acquired with no thread alive to release it
    lock._probe.acquire()
    with lock:
        pass
    assert lock._pid == os.getpid()


def test_fork_safe_lock_concurrent_recovery():
    """Multiple threads converge on a single recovered lock after fork."""
    lock = _ForkSafeLock()
    lock._pid = -1
    barrier = threading.Barrier(5)
    results = []

    def recover_and_record():
        barrier.wait()
        with lock:
            results.append(id(lock._lock))

    threads = [threading.Thread(target=recover_and_record) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    for t in threads:
        assert not t.is_alive(), "thread did not finish"

    # All threads must have used the same recovered lock
    assert len(set(results)) == 1


# ---------------------------------------------------------------------------
# Span._validate() coverage
# ---------------------------------------------------------------------------


def test_span_validate_rejects_missing_start_time():
    """Span without start_time_ns is rejected during validation."""
    span = Span(trace_id=new_trace_id(), name="test", end_time_ns=now_ns())
    with pytest.raises(PicotelConfigError, match="start_time_ns"):
        span._validate()


def test_span_validate_rejects_missing_end_time():
    """Span without end_time_ns is rejected during validation."""
    span = Span(trace_id=new_trace_id(), name="test", start_time_ns=now_ns())
    with pytest.raises(PicotelConfigError, match="end_time_ns"):
        span._validate()
