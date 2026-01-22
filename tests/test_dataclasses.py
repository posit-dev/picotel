"""Tests for Resource and InstrumentationScope dataclasses."""

from miniotel import InstrumentationScope, Resource


def test_resource_with_attributes():
    """Resource stores service metadata attributes."""
    r = Resource({"service.name": "test"})
    assert r.attributes["service.name"] == "test"


def test_resource_with_multiple_attributes():
    """Resource can hold multiple attributes."""
    r = Resource(
        {
            "service.name": "myapp",
            "service.version": "1.0.0",
            "deployment.environment": "production",
        }
    )
    assert r.attributes["service.name"] == "myapp"
    assert r.attributes["service.version"] == "1.0.0"
    assert r.attributes["deployment.environment"] == "production"


def test_resource_with_keyword_argument():
    """Resource can be instantiated with keyword argument."""
    r = Resource(attributes={"service.name": "kwarg-test"})
    assert r.attributes["service.name"] == "kwarg-test"


def test_instrumentation_scope_basic():
    """InstrumentationScope requires name, optional version and attributes."""
    s = InstrumentationScope("mylib", "1.0")
    assert s.name == "mylib"
    assert s.version == "1.0"


def test_instrumentation_scope_defaults():
    """InstrumentationScope has sensible defaults for version and attributes."""
    s = InstrumentationScope("lib-only")
    assert s.name == "lib-only"
    assert s.version == ""
    assert s.attributes == {}


def test_instrumentation_scope_with_attributes():
    """InstrumentationScope can store attributes."""
    s = InstrumentationScope("lib", "2.0", {"custom.key": "value"})
    assert s.name == "lib"
    assert s.version == "2.0"
    assert s.attributes == {"custom.key": "value"}


def test_instrumentation_scope_keyword_arguments():
    """InstrumentationScope can be instantiated with keyword arguments."""
    s = InstrumentationScope(name="kwlib", version="3.0", attributes={"debug": True})
    assert s.name == "kwlib"
    assert s.version == "3.0"
    assert s.attributes == {"debug": True}


def test_instrumentation_scope_default_attributes_isolation():
    """Each InstrumentationScope instance has its own attributes dict."""
    s1 = InstrumentationScope("lib1")
    s2 = InstrumentationScope("lib2")
    s1.attributes["key"] = "value"
    assert "key" not in s2.attributes
