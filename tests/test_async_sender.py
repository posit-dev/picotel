"""Tests for _AsyncSender background dispatch."""

import logging
import os
import threading
from unittest.mock import Mock, patch

import picotel
from picotel import (
    Resource,
    Span,
    _AsyncSender,
    new_span_id,
    new_trace_id,
    now_ns,
)

_mock_response = Mock(status=200)
_mock_response.__enter__ = Mock(return_value=_mock_response)
_mock_response.__exit__ = Mock(return_value=False)

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

    # Unblock the worker so the queue drains
    release.set()
    done = threading.Event()
    # Wait until a submit succeeds — that resets the guard
    assert done.wait(timeout=2) or True  # just a small delay
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


def test_submit_returns_false_when_disabled():
    """submit() returns False and never starts a thread when SDK is disabled."""
    with patch.dict(os.environ, {"OTEL_SDK_DISABLED": "true"}):
        picotel._is_disabled.cache_clear()
        sender = _AsyncSender()
        done = threading.Event()

        assert sender.submit(done.set) is False
        assert sender._thread is None
        assert not done.is_set(), "callable should not have been executed"


# ---------------------------------------------------------------------------
# Integration: Span.__exit__ and OTLPHandler.emit deliver via async
# ---------------------------------------------------------------------------


def test_span_exit_delivers_via_async(monkeypatch):
    """Span.__exit__ delivers spans through the background sender."""
    called = threading.Event()

    def _urlopen_and_signal(*_args, **_kwargs):
        called.set()
        return _mock_response

    mock_urlopen = Mock(side_effect=_urlopen_and_signal)
    monkeypatch.setattr(picotel.urllib.request, "urlopen", mock_urlopen)
    picotel._sender = _AsyncSender()

    resource = Resource(attributes={"service.name": "test"})
    with Span(
        trace_id=new_trace_id(),
        span_id=new_span_id(),
        name="async-test",
        start_time_ns=now_ns(),
        resource=resource,
        endpoint="http://collector:4318",
    ):
        pass

    assert called.wait(timeout=5), "span not delivered"
    request = mock_urlopen.call_args[0][0]
    assert request.get_full_url() == "http://collector:4318/v1/traces"


def test_otlp_handler_emit_delivers_via_async(monkeypatch):
    """OTLPHandler.emit delivers logs through the background sender."""
    called = threading.Event()

    def _urlopen_and_signal(*_args, **_kwargs):
        called.set()
        return _mock_response

    mock_urlopen = Mock(side_effect=_urlopen_and_signal)
    monkeypatch.setattr(picotel.urllib.request, "urlopen", mock_urlopen)
    picotel._sender = _AsyncSender()

    handler = picotel.OTLPHandler(
        resource=Resource(attributes={"service.name": "test"}),
        endpoint="http://collector:4318",
    )
    logger = logging.getLogger("test_async_handler")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    try:
        logger.info("hello from async test")
        assert called.wait(timeout=5), "log not delivered"
        request = mock_urlopen.call_args[0][0]
        assert request.get_full_url() == "http://collector:4318/v1/logs"
    finally:
        logger.removeHandler(handler)


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
