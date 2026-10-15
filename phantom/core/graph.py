"""
Causal dependency graph and partial-order analysis for distributed traces.
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Set, Tuple
import networkx as nx

from phantom.core.event import Event, EventType, VectorClock
from phantom.core.trace import Trace


class CausalGraph:
    """
    Directed Acyclic Graph (DAG) representing happens-before (->) relations:
    - Program order: consecutive events on the same node/thread
    - Message order: SEND(m) -> RECEIVE(m)
    - Data flow: WRITE(k) -> READ(k)
    - Fault dependencies: FAULT -> impacted operations
    - Explicit dependencies: event.depends_on
    """

    def __init__(self, trace: Trace):
        self.trace = trace
        self.graph = nx.DiGraph()
        self._build_graph()

    def _build_graph(self) -> None:
        """Construct the causal DAG from trace events and metadata."""
        # Add all events as nodes
        for event in self.trace.events:
            self.graph.add_node(
                event.event_id,
                event=event,
                node=event.node,
                event_type=event.event_type.value,
                logical_time=event.logical_time,
            )

        # 1. Program order per node
        node_events: Dict[str, List[Event]] = {}
        for event in self.trace.events:
            node_events.setdefault(event.node, []).append(event)

        for n, evts in node_events.items():
            for i in range(len(evts) - 1):
                self.graph.add_edge(
                    evts[i].event_id,
                    evts[i + 1].event_id,
                    edge_type="program_order",
                )

        # 2. Explicit dependencies
        for event in self.trace.events:
            for dep_id in event.depends_on:
                if self.graph.has_node(dep_id):
                    self.graph.add_edge(dep_id, event.event_id, edge_type="explicit_dep")

        # 3. Message order: SEND -> RECEIVE matching
        message_sends: Dict[str, Event] = {}
        for event in self.trace.events:
            if event.event_type == EventType.SEND:
                msg_id = event.payload.get("msg_id") or event.payload.get("message_id")
                if msg_id:
                    message_sends[str(msg_id)] = event
            elif event.event_type == EventType.RECEIVE:
                msg_id = event.payload.get("msg_id") or event.payload.get("message_id")
                if msg_id and str(msg_id) in message_sends:
                    send_evt = message_sends[str(msg_id)]
                    self.graph.add_edge(
                        send_evt.event_id,
                        event.event_id,
                        edge_type="message_order",
                    )

        # 4. Data flow (Read-After-Write) matching
        latest_writes: Dict[str, Event] = {}
        for event in self.trace.events:
            if event.event_type == EventType.WRITE:
                key = event.payload.get("key") or event.payload.get("state_key")
                if key:
                    latest_writes[str(key)] = event
            elif event.event_type == EventType.READ:
                key = event.payload.get("key") or event.payload.get("state_key")
                observed_val = event.payload.get("val") or event.payload.get("value")
                if key and str(key) in latest_writes:
                    prev_write = latest_writes[str(key)]
                    # If read specifies observed value, ensure it matches
                    w_val = prev_write.payload.get("val") or prev_write.payload.get("value")
                    if observed_val is None or observed_val == w_val:
                        self.graph.add_edge(
                            prev_write.event_id,
                            event.event_id,
                            edge_type="data_flow",
                        )

        # 5. Vector clock partial order (Fidge-Mattern / Lamport)
        events_with_vc = [e for e in self.trace.events if e.vector_clock and e.vector_clock.clock]
        for i in range(len(events_with_vc)):
            e1 = events_with_vc[i]
            for j in range(i + 1, len(events_with_vc)):
                e2 = events_with_vc[j]
                if e1.vector_clock.happens_before(e2.vector_clock):
                    if not self.graph.has_edge(e1.event_id, e2.event_id):
                        self.graph.add_edge(e1.event_id, e2.event_id, edge_type="vector_clock")
                elif e2.vector_clock.happens_before(e1.vector_clock):
                    if not self.graph.has_edge(e2.event_id, e1.event_id):
                        self.graph.add_edge(e2.event_id, e1.event_id, edge_type="vector_clock")

        # 6. Connect failure event to latest preceding events across nodes
        if self.trace.failure_event_id and self.graph.has_node(self.trace.failure_event_id):
            fail_evt = self.trace.get_event(self.trace.failure_event_id)
            if fail_evt and self.graph.in_degree(self.trace.failure_event_id) == 0:
                # Connect to preceding events on the same node
                for prev in self.trace.events:
                    if prev.event_id == fail_evt.event_id:
                        break
                    if prev.node == fail_evt.node:
                        self.graph.add_edge(prev.event_id, fail_evt.event_id, edge_type="assertion_dep")
                # If still 0 in-degree, connect to preceding state writes or faults
                if self.graph.in_degree(self.trace.failure_event_id) == 0:
                    for prev in self.trace.events:
                        if prev.event_id == fail_evt.event_id:
                            break
                        if prev.is_state_access() or prev.is_fault():
                            self.graph.add_edge(prev.event_id, fail_evt.event_id, edge_type="assertion_dep")

        # 7. Cycle resolution: ensure graph is a strict DAG even if clocks were skewed or corrupt
        if not nx.is_directed_acyclic_graph(self.graph):
            self._resolve_cycles_heuristically()

    def _resolve_cycles_heuristically(self) -> None:
        """Break cycles introduced by clock skew or corrupt logs to maintain DAG property."""
        trace_order = {e.event_id: i for i, e in enumerate(self.trace.events)}
        try:
            while not nx.is_directed_acyclic_graph(self.graph):
                cycle = nx.find_cycle(self.graph, orientation="original")
                worst_edge = None
                worst_diff = -float("inf")
                for u, v, _ in cycle:
                    diff = trace_order.get(u, 0) - trace_order.get(v, 0)
                    if diff > worst_diff:
                        worst_diff = diff
                        worst_edge = (u, v)
                if worst_edge and self.graph.has_edge(*worst_edge):
                    self.graph.remove_edge(*worst_edge)
                else:
                    # Break first edge in cycle if heuristic didn't find one
                    u, v, _ = cycle[0]
                    self.graph.remove_edge(u, v)
        except Exception:
            pass

    def causal_cone(self, target_event_id: str) -> Set[str]:
        """
        Backward slice: compute all events that causally precede target_event_id.
        C(target) = {e | e ->* target} U {target}.
        """
        if not self.graph.has_node(target_event_id):
            return set()
        ancestors = nx.ancestors(self.graph, target_event_id)
        ancestors.add(target_event_id)
        return ancestors

    def forward_cone(self, source_event_id: str) -> Set[str]:
        """
        Forward slice: compute all events causally influenced by source_event_id.
        F(source) = {e | source ->* e} U {source}.
        """
        if not self.graph.has_node(source_event_id):
            return set()
        descendants = nx.descendants(self.graph, source_event_id)
        descendants.add(source_event_id)
        return descendants

    def are_concurrent(self, event_id_a: str, event_id_b: str) -> bool:
        """
        Two events are concurrent iff neither has a causal path to the other.
        """
        if not self.graph.has_node(event_id_a) or not self.graph.has_node(event_id_b):
            return False
        if event_id_a == event_id_b:
            return False
        has_a_to_b = nx.has_path(self.graph, event_id_a, event_id_b)
        has_b_to_a = nx.has_path(self.graph, event_id_b, event_id_a)
        return not (has_a_to_b or has_b_to_a)

    def find_conflicting_pairs(self) -> List[Tuple[Event, Event]]:
        """
        Identify pairs of concurrent events that access the same shared state,
        where at least one is a WRITE (race condition candidates).
        """
        conflicts = []
        events = [self.graph.nodes[nid]["event"] for nid in self.graph.nodes]
        for i in range(len(events)):
            e1 = events[i]
            if not e1.is_state_access():
                continue
            k1 = e1.payload.get("key") or e1.payload.get("state_key")
            if not k1:
                continue

            for j in range(i + 1, len(events)):
                e2 = events[j]
                if not e2.is_state_access():
                    continue
                k2 = e2.payload.get("key") or e2.payload.get("state_key")
                if k1 != k2:
                    continue

                # At least one write
                if e1.event_type == EventType.WRITE or e2.event_type == EventType.WRITE:
                    if self.are_concurrent(e1.event_id, e2.event_id):
                        conflicts.append((e1, e2))
        return conflicts

    def is_causally_closed(self, subset_ids: Set[str]) -> bool:
        """
        Check if a subset of event IDs is causally closed (downward-closed):
        If B in subset and A -> B, is A in subset?
        (Useful for constructing valid execution prefixes).
        """
        for eid in subset_ids:
            if not self.graph.has_node(eid):
                continue
            for pred in self.graph.predecessors(eid):
                if pred not in subset_ids:
                    return False
        return True

    def topological_order(self, subset_ids: Optional[Set[str]] = None) -> List[str]:
        """Return a topologically sorted list of event IDs."""
        if subset_ids is None:
            subgraph = self.graph
        else:
            subgraph = self.graph.subgraph(subset_ids)
        try:
            return list(nx.topological_sort(subgraph))
        except nx.NetworkXUnfeasible:
            # Fallback to logical time and trace index if subgraph has cycles
            nodes = list(subgraph.nodes())
            trace_indices = {e.event_id: idx for idx, e in enumerate(self.trace.events)}
            return sorted(
                nodes,
                key=lambda nid: (
                    subgraph.nodes[nid].get("logical_time", 0) if "logical_time" in subgraph.nodes[nid] else 0,
                    trace_indices.get(nid, 0),
                ),
            )

    def critical_path_to_failure(self) -> List[str]:
        """Compute the longest causal path ending at failure_event_id."""
        if not self.trace.failure_event_id or not self.graph.has_node(self.trace.failure_event_id):
            return []
        cone = self.causal_cone(self.trace.failure_event_id)
        sub = self.graph.subgraph(cone)
        try:
            return nx.dag_longest_path(sub)
        except Exception:
            trace_indices = {e.event_id: idx for idx, e in enumerate(self.trace.events)}
            return sorted(
                list(cone),
                key=lambda nid: (
                    self.graph.nodes[nid].get("logical_time", 0) if self.graph.has_node(nid) else 0,
                    trace_indices.get(nid, 0),
                ),
            )

    def stats(self) -> Dict[str, Any]:
        return {
            "num_nodes": self.graph.number_of_nodes(),
            "num_edges": self.graph.number_of_edges(),
            "is_dag": nx.is_directed_acyclic_graph(self.graph),
            "density": nx.density(self.graph),
        }
