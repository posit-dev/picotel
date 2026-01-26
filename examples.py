#!/usr/bin/env python3

"""
Examples demonstrating picotel usage.

These examples show common patterns for adding observability to Python applications
using the picotel minimal OpenTelemetry client.
"""

from picotel import (
    Span,
    LogRecord,
    Resource,
    OTLPHandler,
    InstrumentationScope,
    new_trace_id,
    new_span_id,
    now_ns,
    send_spans,
    send_logs,
    TRACEPARENT,
)
import logging
import time
import random


# -----------------------------------------------------------------------------
# Example 1: Basic Tracing with Context Manager
# -----------------------------------------------------------------------------

def example_basic_tracing():
    """Simple example using context manager for automatic timing and sending."""

    # Configure your service
    resource = Resource({
        "service.name": "example-service",
        "service.version": "1.0.0",
        "deployment.environment": "development"
    })

    # Use context manager for automatic timing
    with Span(
        trace_id=new_trace_id(),
        name="example-operation",
        start_time_ns=0,  # 0 means "set automatically"
        end_time_ns=0,
        endpoint="http://localhost:4318",
        resource=resource,
        kind=Span.Kind.INTERNAL,
        attributes={
            "example.type": "basic",
            "example.user_id": "user123"
        }
    ) as span:
        # Simulate some work
        time.sleep(0.1)

        # Add attributes during execution
        span.attributes["example.items_processed"] = 42
        span.attributes["example.status"] = "success"

        # Add an event
        span.events.append(Span.Event(
            name="checkpoint",
            timestamp_ns=now_ns(),
            attributes={"checkpoint.name": "halfway"}
        ))

        # Set final status
        span.status = Span.Status.OK


# -----------------------------------------------------------------------------
# Example 2: Parent-Child Spans (Nested Operations)
# -----------------------------------------------------------------------------

def example_nested_spans():
    """Example showing parent-child relationship between spans."""

    resource = Resource({"service.name": "order-service"})
    endpoint = "http://localhost:4318"
    trace_id = new_trace_id()

    # Parent span: HTTP request handler
    with Span(
        trace_id=trace_id,
        name="POST /api/orders",
        start_time_ns=0,
        end_time_ns=0,
        endpoint=endpoint,
        resource=resource,
        kind=Span.Kind.SERVER,
        attributes={
            "http.method": "POST",
            "http.route": "/api/orders",
            "http.target": "/api/orders?priority=high"
        }
    ) as http_span:

        # Child span 1: Validate input
        with Span(
            trace_id=trace_id,
            parent_span_id=http_span.span_id,
            name="validate-order",
            start_time_ns=0,
            end_time_ns=0,
            endpoint=endpoint,
            resource=resource,
            kind=Span.Kind.INTERNAL
        ) as validation_span:
            time.sleep(0.01)
            validation_span.attributes["validation.result"] = "passed"

        # Child span 2: Database operation
        with Span(
            trace_id=trace_id,
            parent_span_id=http_span.span_id,
            name="INSERT orders",
            start_time_ns=0,
            end_time_ns=0,
            endpoint=endpoint,
            resource=resource,
            kind=Span.Kind.CLIENT,
            attributes={
                "db.system": "postgresql",
                "db.name": "shop",
                "db.operation": "INSERT",
                "db.statement": "INSERT INTO orders (customer_id, total) VALUES (?, ?)"
            }
        ) as db_span:
            time.sleep(0.05)
            db_span.attributes["db.rows_affected"] = 1

        # Child span 3: Send notification
        with Span(
            trace_id=trace_id,
            parent_span_id=http_span.span_id,
            name="send-notification",
            start_time_ns=0,
            end_time_ns=0,
            endpoint=endpoint,
            resource=resource,
            kind=Span.Kind.PRODUCER,
            attributes={
                "messaging.system": "kafka",
                "messaging.destination": "order-events"
            }
        ) as notification_span:
            time.sleep(0.02)
            notification_span.attributes["messaging.message_id"] = "msg-12345"

        # Set final HTTP response attributes
        http_span.attributes["http.status_code"] = 201
        http_span.attributes["http.response_size"] = 256
        http_span.status = Span.Status.OK


# -----------------------------------------------------------------------------
# Example 3: Error Handling and Status Reporting
# -----------------------------------------------------------------------------

def example_error_handling():
    """Example showing how to handle and report errors in spans."""

    resource = Resource({"service.name": "payment-service"})

    with Span(
        trace_id=new_trace_id(),
        name="process-payment",
        start_time_ns=0,
        end_time_ns=0,
        endpoint="http://localhost:4318",
        resource=resource,
        kind=Span.Kind.INTERNAL,
        attributes={
            "payment.amount": 99.99,
            "payment.currency": "USD",
            "payment.method": "credit_card"
        }
    ) as span:
        try:
            # Simulate payment processing
            if random.random() > 0.7:  # 30% chance of failure
                raise ValueError("Payment declined: Insufficient funds")

            # Success path
            span.attributes["payment.transaction_id"] = "txn_" + new_span_id()[:8]
            span.attributes["payment.status"] = "completed"
            span.status = Span.Status.OK

        except Exception as e:
            # Error path
            span.status = Span.Status.ERROR
            span.attributes["error.type"] = type(e).__name__
            span.attributes["error.message"] = str(e)
            span.attributes["payment.status"] = "failed"

            # Add error event with details
            span.events.append(Span.Event(
                name="exception",
                timestamp_ns=now_ns(),
                attributes={
                    "exception.type": type(e).__name__,
                    "exception.message": str(e),
                    "exception.escaped": True
                }
            ))


# -----------------------------------------------------------------------------
# Example 4: Logging with Trace Correlation
# -----------------------------------------------------------------------------

def example_correlated_logging():
    """Example showing logs correlated with traces."""

    resource = Resource({
        "service.name": "user-service",
        "service.version": "2.0.0"
    })
    endpoint = "http://localhost:4318"

    # Configure Python logging to send to OTLP
    handler = OTLPHandler(endpoint, resource)
    logger = logging.getLogger("user-service")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    # Create a traced operation
    trace_id = new_trace_id()

    with Span(
        trace_id=trace_id,
        name="create-user",
        start_time_ns=0,
        end_time_ns=0,
        endpoint=endpoint,
        resource=resource,
        kind=Span.Kind.INTERNAL,
        attributes={"user.email": "user@example.com"}
    ) as span:

        # Log with trace correlation
        logger.info(
            "Creating new user account",
            extra={
                "trace_id": trace_id,
                "span_id": span.span_id,
                "user.email": "user@example.com"
            }
        )

        # Simulate user creation steps
        time.sleep(0.05)

        # Log another correlated message
        logger.info(
            "User account created successfully",
            extra={
                "trace_id": trace_id,
                "span_id": span.span_id,
                "user.id": "usr_12345"
            }
        )

        span.attributes["user.id"] = "usr_12345"
        span.status = Span.Status.OK

    # You can also send logs directly without Python logging
    log = LogRecord(
        body="User verification email sent",
        severity_number=LogRecord.Severity.INFO,
        severity_text="INFO",
        trace_id=trace_id,
        span_id=span.span_id,
        attributes={
            "user.id": "usr_12345",
            "email.type": "verification",
            "email.provider": "sendgrid"
        }
    )
    send_logs(endpoint, resource, [log])


# -----------------------------------------------------------------------------
# Example 5: Manual Span Creation and Batch Sending
# -----------------------------------------------------------------------------

def example_manual_spans():
    """Example showing manual span creation and batch sending."""

    resource = Resource({
        "service.name": "batch-processor",
        "service.instance_id": "worker-001"
    })

    scope = InstrumentationScope(
        name="picotel-examples",
        version="1.0.0"
    )

    trace_id = new_trace_id()
    spans = []

    # Create multiple spans manually
    start_batch = now_ns()

    # Root span for the batch
    batch_span = Span(
        trace_id=trace_id,
        span_id=new_span_id(),
        name="process-batch",
        start_time_ns=start_batch,
        end_time_ns=0,  # Will set later
        kind=Span.Kind.INTERNAL,
        attributes={
            "batch.size": 3,
            "batch.id": "batch_001"
        }
    )

    # Process individual items
    for i in range(3):
        item_start = now_ns()

        # Simulate processing
        time.sleep(random.uniform(0.01, 0.05))

        item_span = Span(
            trace_id=trace_id,
            span_id=new_span_id(),
            parent_span_id=batch_span.span_id,
            name=f"process-item-{i}",
            start_time_ns=item_start,
            end_time_ns=now_ns(),
            kind=Span.Kind.INTERNAL,
            attributes={
                "item.index": i,
                "item.id": f"item_{i:03d}",
                "item.processing_time_ms": (now_ns() - item_start) / 1_000_000
            }
        )
        spans.append(item_span)

    # Finish batch span
    batch_span.end_time_ns = now_ns()
    batch_span.attributes["batch.duration_ms"] = (batch_span.end_time_ns - start_batch) / 1_000_000
    batch_span.status = Span.Status.OK
    spans.insert(0, batch_span)  # Add parent span first

    # Send all spans at once
    success = send_spans("http://localhost:4318", resource, spans, scope)
    print(f"Batch send {'succeeded' if success else 'failed'}")


# -----------------------------------------------------------------------------
# Example 6: Using Environment Variables
# -----------------------------------------------------------------------------

def example_env_configuration():
    """Example using environment variables for configuration.

    Before running, set:
    export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
    export OTEL_SERVICE_NAME=my-service
    export OTEL_EXPORTER_OTLP_HEADERS="api-key=secret123"
    """

    # When env vars are set, you can use simplified API
    with Span(
        trace_id=new_trace_id(),
        name="env-configured-span",
        start_time_ns=0,
        end_time_ns=0,
        attributes={"config.source": "environment"}
    ) as span:
        # The span will be sent using env var configuration
        span.attributes["result"] = "success"

    # Or send manually using env vars
    span = Span(
        trace_id=new_trace_id(),
        span_id=new_span_id(),
        name="manual-send",
        start_time_ns=now_ns(),
        end_time_ns=now_ns() + 1_000_000,
        attributes={"method": "manual"}
    )

    # send() method uses env vars when endpoint/resource not provided
    span.send()


# -----------------------------------------------------------------------------
# Example 7: Trace Context Propagation
# -----------------------------------------------------------------------------

def example_trace_propagation():
    """Example continuing a trace from TRACEPARENT environment variable.

    This is useful in microservices where trace context is propagated
    via environment variables in containers or serverless functions.

    Before running, set:
    export TRACEPARENT=00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01
    """

    resource = Resource({"service.name": "downstream-service"})

    # Continue trace from TRACEPARENT env var
    with Span(
        trace_id=TRACEPARENT,  # Reads from TRACEPARENT env var
        name="downstream-operation",
        start_time_ns=0,
        end_time_ns=0,
        endpoint="http://localhost:4318",
        resource=resource,
        kind=Span.Kind.INTERNAL,
        attributes={
            "propagation.source": "environment",
            "operation.type": "downstream"
        }
    ) as span:
        # This span continues the trace from upstream service
        time.sleep(0.1)
        span.attributes["result"] = "processed"
        span.status = Span.Status.OK


# -----------------------------------------------------------------------------
# Example 8: Span Links
# -----------------------------------------------------------------------------

def example_span_links():
    """Example demonstrating span links for batch processing scenarios."""

    resource = Resource({"service.name": "batch-aggregator"})

    # Create some source traces that will be linked
    source_trace_ids = [new_trace_id() for _ in range(3)]
    source_span_ids = [new_span_id() for _ in range(3)]

    # Create a batch processing span that links to source spans
    with Span(
        trace_id=new_trace_id(),
        name="aggregate-orders",
        start_time_ns=0,
        end_time_ns=0,
        endpoint="http://localhost:4318",
        resource=resource,
        kind=Span.Kind.INTERNAL,
        attributes={
            "batch.type": "order_aggregation",
            "batch.source_count": len(source_trace_ids)
        }
    ) as span:

        # Add links to source spans
        for i, (trace_id, span_id) in enumerate(zip(source_trace_ids, source_span_ids)):
            span.links.append(Span.Link(
                trace_id=trace_id,
                span_id=span_id,
                attributes={
                    "link.type": "source_order",
                    "link.index": i
                }
            ))

        # Simulate aggregation work
        time.sleep(0.1)

        span.attributes["batch.result_count"] = 1
        span.attributes["batch.total_amount"] = 299.97
        span.status = Span.Status.OK


# -----------------------------------------------------------------------------
# Main: Run Examples
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    print("Running picotel examples...")
    print("Make sure OTLP collector is running at http://localhost:4318")
    print("-" * 60)

    examples = [
        ("Basic Tracing", example_basic_tracing),
        ("Nested Spans", example_nested_spans),
        ("Error Handling", example_error_handling),
        ("Correlated Logging", example_correlated_logging),
        ("Manual Spans", example_manual_spans),
        ("Span Links", example_span_links),
    ]

    for name, func in examples:
        print(f"\nRunning: {name}")
        try:
            func()
            print(f"  ✓ {name} completed")
        except Exception as e:
            print(f"  ✗ {name} failed: {e}")

    print("\n" + "-" * 60)
    print("Examples complete. Check your OTLP collector for traces and logs.")