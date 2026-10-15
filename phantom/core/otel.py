"""
OpenTelemetry and Jaeger trace importer and exporter for Project PHANTOM.
Ingests structured distributed trace spans, service logs, and parent-child contexts,
mapping them into causal PHANTOM execution traces and happens-before partial orders.
"""

from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from phantom.core.event import Event, EventType, VectorClock
from phantom.core.trace import Trace


class OTelTraceImporter:
    """
    Parses OpenTelemetry JSON and Jaeger trace exports into PHANTOM Trace objects.
    Extracts distributed service calls, span events, exceptions, and parent-child dependencies.
    """

    @staticmethod
    def from_otel_json(data: Dict[str, Any] | List[Dict[str, Any]] | str | Path) -> Trace:
        """
        Parse OpenTelemetry trace data from JSON dict, file path, or string.
        Supports standard OTLP JSON (resourceSpans) and Jaeger span list formats.
        """
        if isinstance(data, (str, Path)):
            p = Path(data)
            if p.exists():
                raw = json.loads(p.read_text(encoding="utf-8"))
            else:
                raw = json.loads(str(data))
        else:
            raw = data

        spans = []
        if isinstance(raw, list):
            spans = raw
        elif isinstance(raw, dict):
            if "data" in raw and isinstance(raw["data"], list) and raw["data"]:
                # Jaeger export format
                spans = raw["data"][0].get("spans", [])
            elif "resourceSpans" in raw:
                # Standard OpenTelemetry OTLP format
                for rs in raw.get("resourceSpans", []):
                    service_name = "unknown_service"
                    for attr in rs.get("resource", {}).get("attributes", []):
                        if attr.get("key") == "service.name":
                            service_name = attr.get("value", {}).get("stringValue", "service")
                    for ss in rs.get("scopeSpans", []):
                        for sp in ss.get("spans", []):
                            sp["_service_name"] = service_name
                            spans.append(sp)
            elif "spans" in raw:
                spans = raw["spans"]

        if not spans:
            return Trace(events=[], failure_signature="EmptyOTelTrace")

        events: List[Event] = []
        failure_event_id: Optional[str] = None
        failure_sig = ""

        # Sort spans by startTimeUnixNano if available, or logical index
        def get_start_time(sp: Dict[str, Any]) -> int:
            return int(sp.get("startTimeUnixNano") or sp.get("startTime") or 0)

        sorted_spans = sorted(spans, key=get_start_time)

        span_id_to_event: Dict[str, str] = {}
        node_vcs: Dict[str, VectorClock] = {}

        for idx, sp in enumerate(sorted_spans, 1):
            span_id = str(sp.get("spanId") or sp.get("spanID") or f"span_{idx}")
            parent_id = str(sp.get("parentSpanId") or sp.get("parentSpanID") or "")
            name = sp.get("name") or sp.get("operationName") or "operation"

            # Resolve service / node
            node = sp.get("_service_name") or sp.get("process", {}).get("serviceName") or "service_node"
            for tag in sp.get("tags", []):
                if tag.get("key") in {"service.name", "peer.service", "node"}:
                    node = tag.get("value")

            # Extract attributes
            attributes = {}
            for tag in sp.get("tags", []):
                attributes[tag.get("key")] = tag.get("value")
            for attr in sp.get("attributes", []):
                key = attr.get("key")
                val = attr.get("value", {})
                if isinstance(val, dict):
                    attributes[key] = next(iter(val.values())) if val else None
                else:
                    attributes[key] = val

            # Determine EventType
            event_type = EventType.INTERNAL
            kind = sp.get("kind", 0)  # 1=INTERNAL, 2=SERVER, 3=CLIENT, 4=PRODUCER, 5=CONSUMER
            if kind in {3, 4} or "send" in name.lower():
                event_type = EventType.SEND
            elif kind in {2, 5} or "recv" in name.lower() or "receive" in name.lower():
                event_type = EventType.RECEIVE
            elif "write" in name.lower() or "set" in name.lower() or "update" in name.lower():
                event_type = EventType.WRITE
            elif "read" in name.lower() or "get" in name.lower():
                event_type = EventType.READ

            # Check status / error
            status = sp.get("status", {})
            is_error = status.get("code") == 2 or attributes.get("error") is True or "error" in attributes

            # Vector clock management
            vc = node_vcs.setdefault(node, VectorClock()).clone()
            vc.increment(node)
            node_vcs[node] = vc

            depends = set()
            if parent_id and parent_id in span_id_to_event:
                depends.add(span_id_to_event[parent_id])

            payload = dict(attributes)
            payload["operation"] = name
            payload["span_id"] = span_id

            clean_span = span_id.lstrip("0") or span_id
            evt_id = f"evt_{clean_span}" if len(clean_span) <= 16 else f"evt_{clean_span[-8:]}"
            if evt_id in span_id_to_event.values():
                evt_id = f"{evt_id}_{idx}"
            span_id_to_event[span_id] = evt_id

            # Detect failure or assertion event
            if is_error:
                error_msg = status.get("message") or attributes.get("error.message") or name
                event_type = EventType.ASSERTION
                payload["signature"] = error_msg
                payload["name"] = name
                failure_event_id = evt_id
                failure_sig = str(error_msg)

            # Check embedded span events (exceptions, logs)
            for ev in sp.get("events", sp.get("logs", [])):
                ev_name = ev.get("name", "")
                if "exception" in ev_name.lower() or "error" in ev_name.lower():
                    event_type = EventType.ASSERTION
                    sig = ev.get("attributes", {}).get("exception.type", "SpanException")
                    payload["signature"] = sig
                    failure_event_id = evt_id
                    failure_sig = str(sig)

            event = Event(
                event_id=evt_id,
                event_type=event_type,
                node=node,
                payload=payload,
                vector_clock=vc,
                logical_time=idx,
                target=attributes.get("peer.service") or attributes.get("target"),
                depends_on=depends,
            )
            events.append(event)

        return Trace(
            events=events,
            failure_event_id=failure_event_id,
            failure_signature=failure_sig or "OTelReportedFailure",
            metadata={"source": "OpenTelemetry_Importer", "raw_spans_count": len(spans)},
        )

    @staticmethod
    def to_jaeger_json(trace: Trace) -> Dict[str, Any]:
        """Convert a PHANTOM Trace into Jaeger JSON format for visualization."""
        spans = []
        trace_id = trace.metadata.get("trace_id", "4bf92f3577b34da6a3ce929d0e0e4736")

        for i, e in enumerate(trace.events, 1):
            span_id = f"{i:016x}"
            tags = [{"key": k, "type": "string", "value": str(v)} for k, v in e.payload.items()]
            tags.append({"key": "event_type", "type": "string", "value": e.event_type.value})
            tags.append({"key": "event_id", "type": "string", "value": e.event_id})

            span = {
                "traceID": trace_id,
                "spanID": span_id,
                "operationName": f"{e.event_type.value}_{e.event_id}",
                "references": [],
                "startTime": e.logical_time * 1000000,
                "duration": 50000,
                "tags": tags,
                "logs": [],
                "process": {"serviceName": e.node},
            }
            if e.event_id == trace.failure_event_id:
                span["tags"].append({"key": "error", "type": "bool", "value": True})
            spans.append(span)

        return {
            "data": [
                {
                    "traceID": trace_id,
                    "spans": spans,
                }
            ]
        }
