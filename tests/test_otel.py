import json
from pathlib import Path
import pytest

from phantom.core.event import EventType
from phantom.core.otel import OTelTraceImporter
from phantom.core.trace import Trace

def test_otel_otlp_json_import():
    otlp_data = {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [{"key": "service.name", "value": {"stringValue": "checkout-svc"}}]
                },
                "scopeSpans": [
                    {
                        "spans": [
                            {
                                "traceId": "trace1",
                                "spanId": "span1",
                                "name": "HTTP POST /checkout",
                                "kind": 2,
                                "startTimeUnixNano": 1000000,
                                "attributes": [{"key": "http.status_code", "value": {"intValue": 200}}],
                            },
                            {
                                "traceId": "trace1",
                                "spanId": "span2",
                                "parentSpanId": "span1",
                                "name": "PaymentClient.charge",
                                "kind": 3,
                                "startTimeUnixNano": 1005000,
                                "status": {"code": 2, "message": "PaymentGatewayTimeout"},
                                "attributes": [{"key": "error", "value": {"boolValue": True}}],
                            },
                        ]
                    }
                ],
            }
        ]
    }

    trace = OTelTraceImporter.from_otel_json(otlp_data)
    assert len(trace.events) == 2
    assert "checkout-svc" in trace.nodes()
    assert trace.failure_signature == "PaymentGatewayTimeout"
    assert trace.failure_event_id is not None

    # Check parent-child dependency
    e2 = trace.get_event(trace.failure_event_id)
    assert e2 is not None
    assert e2.event_type == EventType.ASSERTION

def test_jaeger_json_import_and_export():
    jaeger_data = {
        "data": [
            {
                "traceID": "jaeger_trace_99",
                "spans": [
                    {
                        "spanID": "sp_root",
                        "operationName": "recv_order",
                        "process": {"serviceName": "api-gateway"},
                        "tags": [{"key": "span.kind", "value": "server"}],
                        "startTime": 1000,
                    },
                    {
                        "spanID": "sp_child",
                        "parentSpanID": "sp_root",
                        "operationName": "write_db",
                        "process": {"serviceName": "db-service"},
                        "tags": [{"key": "error", "value": True}],
                        "logs": [{"fields": [{"key": "event", "value": "error"}]}],
                        "startTime": 2000,
                    },
                ],
            }
        ]
    }

    trace = OTelTraceImporter.from_otel_json(jaeger_data)
    assert len(trace.events) == 2
    assert "api-gateway" in trace.nodes()
    assert "db-service" in trace.nodes()

    # Test roundtrip back to Jaeger JSON
    jaeger_out = OTelTraceImporter.to_jaeger_json(trace)
    assert "data" in jaeger_out
    assert len(jaeger_out["data"][0]["spans"]) == 2
