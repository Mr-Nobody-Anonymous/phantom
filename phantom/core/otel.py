"""
OpenTelemetry and Jaeger trace importer and exporter for Project PHANTOM.
Enhanced with semantic attribute mapping (DB, messaging, RPC, HTTP),
W3C context propagation, and missing-data diagnostics (incomplete spans, clock skew, missing parents).
"""

from __future__ import annotations
from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from phantom.core.event import Event, EventType, VectorClock
from phantom.core.trace import Trace


@dataclass
class OTelDiagnostics:
    """Detailed diagnostics of OpenTelemetry trace quality and missing evidence."""
    incomplete_spans: List[Dict[str, Any]] = field(default_factory=list)
    missing_parents: List[Dict[str, Any]] = field(default_factory=list)
    clock_skew_anomalies: List[Dict[str, Any]] = field(default_factory=list)
    unobserved_state_dependencies: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "incomplete_spans": self.incomplete_spans,
            "missing_parents": self.missing_parents,
            "clock_skew_anomalies": self.clock_skew_anomalies,
            "unobserved_state_dependencies": self.unobserved_state_dependencies,
        }


class OTelTraceImporter:
    """
    Parses OpenTelemetry JSON and Jaeger trace exports into PHANTOM Trace objects.
    Extracts distributed service calls, span events, exceptions, and parent-child dependencies
    with semantic mapping for Database, Messaging, HTTP, and RPC conventions.
    """

    @staticmethod
    def parse_traceparent(header_val: str) -> Optional[Tuple[str, str]]:
        """Parse W3C traceparent header (format: 00-{trace_id}-{parent_id}-{flags})."""
        parts = str(header_val).strip().split("-")
        if len(parts) >= 3:
            return parts[1], parts[2]
        return None

    @classmethod
    def from_otel_json(
        cls,
        data: Dict[str, Any] | List[Dict[str, Any]] | str | Path,
    ) -> Trace:
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

        spans: List[Dict[str, Any]] = []
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
                            val_dict = attr.get("value", {})
                            service_name = val_dict.get("stringValue", "service")
                    for ss in rs.get("scopeSpans", []):
                        for sp in ss.get("spans", []):
                            sp["_service_name"] = service_name
                            spans.append(sp)
            elif "spans" in raw:
                spans = raw["spans"]

        diagnostics = OTelDiagnostics()

        if not spans:
            return Trace(
                events=[],
                failure_signature="EmptyOTelTrace",
                metadata={"otel_diagnostics": diagnostics.to_dict()},
            )

        events: List[Event] = []
        failure_event_id: Optional[str] = None
        failure_sig = ""

        def get_start_time(sp: Dict[str, Any]) -> int:
            return int(sp.get("startTimeUnixNano") or sp.get("startTime") or 0)

        def get_end_time(sp: Dict[str, Any]) -> int:
            return int(sp.get("endTimeUnixNano") or sp.get("endTime") or 0)

        sorted_spans = sorted(spans, key=get_start_time)
        all_span_ids = {str(sp.get("spanId") or sp.get("spanID") or f"span_{i}") for i, sp in enumerate(sorted_spans, 1)}

        span_id_to_event: Dict[str, str] = {}
        span_id_to_start_time: Dict[str, int] = {}
        node_vcs: Dict[str, VectorClock] = {}

        for idx, sp in enumerate(sorted_spans, 1):
            span_id = str(sp.get("spanId") or sp.get("spanID") or f"span_{idx}")
            parent_id = str(sp.get("parentSpanId") or sp.get("parentSpanID") or "")
            name = sp.get("name") or sp.get("operationName") or "operation"
            start_t = get_start_time(sp)
            end_t = get_end_time(sp)
            span_id_to_start_time[span_id] = start_t

            # Check for incomplete span
            if end_t != 0 and end_t < start_t:
                diagnostics.incomplete_spans.append({"span_id": span_id, "start": start_t, "end": end_t})

            # Check for missing parent span
            if parent_id and parent_id not in all_span_ids:
                diagnostics.missing_parents.append({"span_id": span_id, "parent_id": parent_id})

            # Check for clock skew anomalies
            if parent_id and parent_id in span_id_to_start_time:
                parent_start = span_id_to_start_time[parent_id]
                if start_t < parent_start:
                    diagnostics.clock_skew_anomalies.append({
                        "span_id": span_id,
                        "parent_id": parent_id,
                        "skew_nanos": parent_start - start_t,
                    })

            # Resolve service / node
            node = sp.get("_service_name") or sp.get("process", {}).get("serviceName") or "service_node"
            for tag in sp.get("tags", []):
                if tag.get("key") in {"service.name", "peer.service", "node"}:
                    node = tag.get("value")

            # Extract attributes
            attributes: Dict[str, Any] = {}
            for tag in sp.get("tags", []):
                attributes[tag.get("key")] = tag.get("value")
            for attr in sp.get("attributes", []):
                key = attr.get("key")
                val = attr.get("value", {})
                if isinstance(val, dict):
                    attributes[key] = next(iter(val.values())) if val else None
                else:
                    attributes[key] = val

            # Check W3C context propagation
            if "traceparent" in attributes:
                parsed = cls.parse_traceparent(attributes["traceparent"])
                if parsed:
                    _, tp_parent = parsed
                    if not parent_id:
                        parent_id = tp_parent

            # Semantic mapping for EventType
            event_type = EventType.INTERNAL
            kind = sp.get("kind", 0)  # 1=INTERNAL, 2=SERVER, 3=CLIENT, 4=PRODUCER, 5=CONSUMER

            # Database semantics
            db_system = attributes.get("db.system")
            db_op = str(attributes.get("db.operation", "")).upper()
            db_statement = str(attributes.get("db.statement", "")).upper()

            # Messaging semantics
            msg_system = attributes.get("messaging.system")
            msg_op = str(attributes.get("messaging.operation", "")).lower()

            if db_system or "db" in attributes:
                if any(op in db_op or op in db_statement for op in ["SELECT", "GET", "FIND", "READ"]):
                    event_type = EventType.READ
                elif any(op in db_op or op in db_statement for op in ["INSERT", "UPDATE", "SET", "DELETE", "WRITE"]):
                    event_type = EventType.WRITE
            elif msg_system or "messaging" in attributes:
                if kind in {3, 4} or msg_op in {"publish", "send", "produce"}:
                    event_type = EventType.SEND
                elif kind in {2, 5} or msg_op in {"receive", "process", "consume"}:
                    event_type = EventType.RECEIVE
            elif kind in {3, 4} or "send" in name.lower():
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

            # Extract state key and store if state operation
            if event_type in {EventType.READ, EventType.WRITE}:
                state_key = (
                    attributes.get("db.sql.table")
                    or attributes.get("db.name")
                    or attributes.get("key")
                    or attributes.get("state_key")
                    or name
                )
                payload["key"] = str(state_key)
                if attributes.get("db.name"):
                    payload["store"] = attributes["db.name"]

            # Extract message ID if messaging operation
            if event_type in {EventType.SEND, EventType.RECEIVE}:
                msg_id = (
                    attributes.get("messaging.message_id")
                    or attributes.get("messaging.conversation_id")
                    or attributes.get("msg_id")
                    or span_id
                )
                payload["msg_id"] = str(msg_id)

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
            metadata={
                "source": "OpenTelemetry_Importer",
                "raw_spans_count": len(spans),
                "otel_diagnostics": diagnostics.to_dict(),
            },
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
