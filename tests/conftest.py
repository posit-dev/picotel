import sys
from pathlib import Path

import pytest

# Add src directory to path so tests can import picotel
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import picotel
from picotel import PicotelConfigError, _logger


class _SyncSender:
    """Synchronous stand-in for _AsyncSender used in most tests.

    Calls the submitted function immediately so tests that mock
    send_spans/send_logs can assert on the mock without race conditions.
    Matches production _AsyncSender exception behavior: config errors are
    logged, all other exceptions are suppressed.
    """

    _thread = None

    def is_alive(self) -> bool:
        return True

    def submit(self, fn, *args, **kwargs) -> bool:
        try:
            fn(*args, **kwargs)
        except PicotelConfigError as e:
            _logger.error(f"Telemetry config error: {e}")
        except Exception:  # noqa: S110
            pass
        return True


@pytest.fixture(autouse=True)
def _clear_picotel_caches():
    """Clear all picotel LRU caches before every test.

    This ensures each test starts from a clean state regardless of
    what previous tests cached from environment variables.
    """
    picotel._prefix.cache_clear()
    picotel._env.cache_clear()
    picotel._is_disabled.cache_clear()
    picotel._get_endpoint.cache_clear()
    picotel._get_resource_from_env.cache_clear()
    picotel._parse_headers.cache_clear()
    picotel._parse_traceparent.cache_clear()
    picotel._sender = _SyncSender()
