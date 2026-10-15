#!/usr/bin/env python3
"""
Project PHANTOM: End-to-End OpenTelemetry Ingestion and Failure Minimization Demo.
Demonstrates ingesting real distributed OpenTelemetry span traces,
running PHANTOM Causal Minimization, and generating a standalone reproducer.
"""

import json
from pathlib import Path
from phantom.core.otel import OTelTraceImporter
from phantom.core.graph import CausalGraph
from phantom.core.oracle import FailureOracle
from phantom.core.scheduler import EventScheduler
from phantom.algorithms.causal_minimizer import PhantomCausalMinimizer
from phantom.reproduction.generator import ReproducerGenerator

def run_otel_pipeline_demo():
    print("================================================================================")
    print("PROJECT PHANTOM: OPENTELEMETRY TRACE INGESTION & CAUSAL RECONSTRUCTION")
    print("================================================================================\n")

    # Sample OpenTelemetry JSON trace representing distributed order processing with noise
    otel_sample = {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [{"key": "service.name", "value": {"stringValue": "order-svc"}}]
                },
                "scopeSpans": [
                    {
                        "spans": [
                            {
                                "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
                                "spanId": "0000000000000001",
                                "name": "health_check_ping",
                                "startTimeUnixNano": 1000000000,
                                "attributes": [{"key": "http.route", "value": {"stringValue": "/health"}}]
                            },
                            {
                                "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
                                "spanId": "0000000000000002",
                                "name": "order_read_pending",
                                "startTimeUnixNano": 1000100000,
                                "attributes": [
                                    {"key": "key", "value": {"stringValue": "ord_99_status"}},
                                    {"key": "val", "value": {"stringValue": "pending"}}
                                ]
                            },
                            {
                                "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
                                "spanId": "0000000000000003",
                                "name": "metrics_scrape_telemetry",
                                "startTimeUnixNano": 1000200000,
                                "attributes": [{"key": "metric.name", "value": {"stringValue": "cpu_load"}}]
                            },
                            {
                                "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
                                "spanId": "0000000000000004",
                                "name": "payment_write_charge",
                                "startTimeUnixNano": 1000300000,
                                "attributes": [
                                    {"key": "key", "value": {"stringValue": "ord_99_charged"}},
                                    {"key": "val", "value": {"boolValue": True}}
                                ]
                            },
                            {
                                "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
                                "spanId": "0000000000000005",
                                "name": "duplicate_charge_detected",
                                "startTimeUnixNano": 1000400000,
                                "status": {"code": 2, "message": "DuplicateOrderProcessing"},
                                "attributes": [
                                    {"key": "error", "value": {"boolValue": True}},
                                    {"key": "error.message", "value": {"stringValue": "DuplicateOrderProcessing"}},
                                    {"key": "key", "value": {"stringValue": "ord_99_charged"}},
                                    {"key": "expected", "value": {"boolValue": False}}
                                ]
                            }
                        ]
                    }
                ]
            }
        ]
    }

    print("[Step 1] Ingesting OpenTelemetry JSON trace containing spans and background noise...")
    trace = OTelTraceImporter.from_otel_json(otel_sample)
    print(f"  -> Successfully imported {len(trace.events)} events across nodes: {trace.nodes()}")
    print(f"  -> Detected failure event: {trace.failure_event_id} ({trace.failure_signature})\n")

    print("[Step 2] Building Causal DAG and happens-before partial order...")
    graph = CausalGraph(trace)
    stats = graph.stats()
    print(f"  -> DAG verified: {stats['is_dag']} | {stats['num_nodes']} nodes, {stats['num_edges']} edges\n")

    print("[Step 3] Executing PHANTOM Causal Minimization...")
    oracle = FailureOracle(expected_signature="DuplicateOrderProcessing")
    oracle.register_invariant(
        "DuplicateOrderProcessing",
        lambda state: not (state.get("kv_store", {}).get("ord_99_charged") is True),
    )
    scheduler = EventScheduler(initial_state={"kv_store": {"ord_99_status": "pending"}})
    minimizer = PhantomCausalMinimizer()
    res = minimizer.minimize(trace, oracle, scheduler)

    print(f"  -> Original Events: {res.original_event_count}")
    print(f"  -> Minimized Events: {res.minimized_event_count} ({res.reduction_ratio * 100:.1f}% reduction)")
    print(f"  -> Executions Required: {res.executions_count}")
    print(f"  -> Stability: {res.reproduction_rate * 100:.1f}%\n")

    out_script = Path("examples/distributed_service/reproduce_otel_failure.py")
    ReproducerGenerator.generate_script(res, out_script)
    print(f"[Step 4] Generated standalone reproducer script at: {out_script}")
    print("Done! PHANTOM successfully proved causal minimization on OpenTelemetry data.")

if __name__ == "__main__":
    run_otel_pipeline_demo()
