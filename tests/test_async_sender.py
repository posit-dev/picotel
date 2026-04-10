"""Tests for _AsyncSender background dispatch."""

import threading

import picotel
from picotel import (
    Resource,
    Span,
    _AsyncSender,
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


def test_lazy_init_thread_is_none_before_first_submit():
    """Worker thread is not created until the first submit."""
    sender = _AsyncSender()
    assert sender._thread is None


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


def test_sync_sender_returns_true():
    """_SyncSender.submit() always returns True."""
    sender = picotel._SyncSender()
    assert sender.submit(lambda: None) is True


def test_sync_sender_logs_config_error(picotel_caplog):
    """_SyncSender logs PicotelConfigError without raising."""
    sender = picotel._SyncSender()

    def raise_config():
        raise picotel.PicotelConfigError("sync config error")

    sender.submit(raise_config)
    assert any("sync config error" in r.message for r in picotel_caplog.records)


def test_sync_sender_suppresses_other_exceptions():
    """_SyncSender suppresses non-config exceptions silently."""
    sender = picotel._SyncSender()
    assert sender.submit(lambda: 1 / 0) is True


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


def test_sync_sender_config_error_does_not_trip():
    """PicotelConfigError does not count toward the circuit breaker."""
    sender = picotel._SyncSender()

    def raise_config():
        raise picotel.PicotelConfigError("no endpoint")

    for _ in range(sender._MAX_CONSECUTIVE_ERRORS + 5):
        sender.submit(raise_config)
    assert sender._tripped is False


def test_sync_sender_is_alive_false_when_tripped():
    """is_alive() returns False once the circuit breaker has tripped."""
    sender = picotel._SyncSender()
    assert sender.is_alive() is True
    for _ in range(sender._MAX_CONSECUTIVE_ERRORS):
        sender.submit(lambda: False)
    assert sender.is_alive() is False


def test_sync_sender_is_alive():
    """_SyncSender.is_alive() returns True when the circuit breaker has not tripped."""
    sender = picotel._SyncSender()
    assert sender.is_alive() is True
