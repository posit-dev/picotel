import sys
from pathlib import Path

import pytest

# Add src directory to path so tests can import picotel
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import picotel


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
