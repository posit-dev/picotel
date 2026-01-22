"""Tests for Span and related dataclasses (Event, Link, SpanStatus)."""

from miniotel import (
    Event,
    Link,
    Span,
    SpanKind,
    SpanStatus,
    StatusCode,
    new_span_id,
    new_trace_id,
    now_ns,
)

SAMPLE_TIMESTAMP = 1234567890
SAMPLE_ROWS_RETURNED = 42


class TestEvent:
    """Tests for Event dataclass."""

    def test_event_basic(self):
        """Event requires name and timestamp_ns."""
        e = Event("cache.hit", SAMPLE_TIMESTAMP)
        assert e.name == "cache.hit"
        assert e.timestamp_ns == SAMPLE_TIMESTAMP
        assert e.attributes == {}

    def test_event_with_attributes(self):
        """Event can store custom attributes."""
        attrs = {"exception.type": "ValueError", "exception.message": "bad input"}
        e = Event("exception", now_ns(), attrs)
        assert e.name == "exception"
        assert e.attributes["exception.type"] == "ValueError"

    def test_event_attributes_isolation(self):
        """Each Event instance has its own attributes dict."""
        e1 = Event("first", 100)
        e2 = Event("second", 200)
        e1.attributes["key"] = "value"
        assert "key" not in e2.attributes


class TestLink:
    """Tests for Link dataclass."""

    def test_link_basic(self):
        """Link requires trace_id and span_id."""
        link = Link("abcd1234abcd1234abcd1234abcd1234", "1234567890abcdef")
        assert link.trace_id == "abcd1234abcd1234abcd1234abcd1234"
        assert link.span_id == "1234567890abcdef"
        assert link.attributes == {}

    def test_link_with_attributes(self):
        """Link can store custom attributes."""
        link = Link(new_trace_id(), new_span_id(), {"link.reason": "retry"})
        assert link.attributes["link.reason"] == "retry"

    def test_link_attributes_isolation(self):
        """Each Link instance has its own attributes dict."""
        l1 = Link("a" * 32, "b" * 16)
        l2 = Link("c" * 32, "d" * 16)
        l1.attributes["key"] = "value"
        assert "key" not in l2.attributes


class TestSpanStatus:
    """Tests for SpanStatus dataclass."""

    def test_status_defaults(self):
        """SpanStatus defaults to UNSET with empty message."""
        s = SpanStatus()
        assert s.code == StatusCode.UNSET
        assert s.message == ""

    def test_status_ok(self):
        """SpanStatus can be set to OK."""
        s = SpanStatus(StatusCode.OK)
        assert s.code == StatusCode.OK

    def test_status_error_with_message(self):
        """SpanStatus ERROR can include a message."""
        s = SpanStatus(StatusCode.ERROR, "Connection timeout")
        assert s.code == StatusCode.ERROR
        assert s.message == "Connection timeout"


class TestSpan:
    """Tests for Span dataclass."""

    def test_span_minimal(self):
        """Span with minimum required fields has correct defaults."""
        trace_id = new_trace_id()
        span_id = new_span_id()
        start = now_ns()
        end = start + 1000000
        span = Span(trace_id, span_id, "test", start, end)

        assert span.trace_id == trace_id
        assert span.span_id == span_id
        assert span.name == "test"
        assert span.start_time_ns == start
        assert span.end_time_ns == end
        assert span.parent_span_id == ""
        assert span.kind == SpanKind.INTERNAL
        assert span.attributes == {}
        assert span.events == []
        assert span.links == []
        assert span.status is None

    def test_span_kind_defaults_to_internal(self):
        """Span.kind defaults to INTERNAL."""
        span = Span(new_trace_id(), new_span_id(), "op", now_ns(), now_ns())
        assert span.kind == SpanKind.INTERNAL

    def test_span_with_parent(self):
        """Span can reference a parent span."""
        parent_span_id = new_span_id()
        span = Span(
            new_trace_id(),
            new_span_id(),
            "child-op",
            now_ns(),
            now_ns(),
            parent_span_id=parent_span_id,
        )
        assert span.parent_span_id == parent_span_id

    def test_span_with_server_kind(self):
        """Span can have explicit kind."""
        span = Span(
            new_trace_id(),
            new_span_id(),
            "http.request",
            now_ns(),
            now_ns(),
            kind=SpanKind.SERVER,
        )
        assert span.kind == SpanKind.SERVER

    def test_span_with_attributes(self):
        """Span can store custom attributes."""
        span = Span(
            new_trace_id(),
            new_span_id(),
            "db.query",
            now_ns(),
            now_ns(),
            attributes={"db.system": "postgresql", "db.statement": "SELECT 1"},
        )
        assert span.attributes["db.system"] == "postgresql"
        assert span.attributes["db.statement"] == "SELECT 1"

    def test_span_with_events(self):
        """Span can contain events."""
        events = [
            Event("query.start", now_ns()),
            Event("query.end", now_ns(), {"rows.returned": SAMPLE_ROWS_RETURNED}),
        ]
        span = Span(
            new_trace_id(),
            new_span_id(),
            "db.query",
            now_ns(),
            now_ns(),
            events=events,
        )
        assert len(span.events) == len(events)
        assert span.events[0].name == "query.start"
        assert span.events[1].attributes["rows.returned"] == SAMPLE_ROWS_RETURNED

    def test_span_with_links(self):
        """Span can contain links to other spans."""
        links = [
            Link(new_trace_id(), new_span_id()),
            Link(new_trace_id(), new_span_id(), {"link.type": "follows_from"}),
        ]
        span = Span(
            new_trace_id(),
            new_span_id(),
            "batch.process",
            now_ns(),
            now_ns(),
            links=links,
        )
        assert len(span.links) == len(links)
        assert span.links[1].attributes["link.type"] == "follows_from"

    def test_span_with_status(self):
        """Span can have a status."""
        span = Span(
            new_trace_id(),
            new_span_id(),
            "failing.op",
            now_ns(),
            now_ns(),
            status=SpanStatus(StatusCode.ERROR, "something went wrong"),
        )
        assert span.status is not None
        assert span.status.code == StatusCode.ERROR
        assert span.status.message == "something went wrong"

    def test_span_attributes_isolation(self):
        """Each Span instance has its own attributes dict."""
        s1 = Span(new_trace_id(), new_span_id(), "op1", now_ns(), now_ns())
        s2 = Span(new_trace_id(), new_span_id(), "op2", now_ns(), now_ns())
        s1.attributes["key"] = "value"
        assert "key" not in s2.attributes

    def test_span_events_isolation(self):
        """Each Span instance has its own events list."""
        s1 = Span(new_trace_id(), new_span_id(), "op1", now_ns(), now_ns())
        s2 = Span(new_trace_id(), new_span_id(), "op2", now_ns(), now_ns())
        s1.events.append(Event("test", now_ns()))
        assert len(s2.events) == 0

    def test_span_links_isolation(self):
        """Each Span instance has its own links list."""
        s1 = Span(new_trace_id(), new_span_id(), "op1", now_ns(), now_ns())
        s2 = Span(new_trace_id(), new_span_id(), "op2", now_ns(), now_ns())
        s1.links.append(Link(new_trace_id(), new_span_id()))
        assert len(s2.links) == 0

    def test_span_keyword_arguments(self):
        """Span can be instantiated with keyword arguments."""
        span = Span(
            trace_id=new_trace_id(),
            span_id=new_span_id(),
            name="keyword-test",
            start_time_ns=now_ns(),
            end_time_ns=now_ns(),
        )
        assert span.name == "keyword-test"
