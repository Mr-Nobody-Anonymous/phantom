"""
Causal dependency graph and partial-order analysis for distributed traces.
Enhanced with multi-store state tracking, version-aware data-flow correlation,
message identity validation, cycle diagnostics, and reachability caching.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple
import networkx as nx

from phantom.core.event import Event, EventType, VectorClock
from phantom.core.trace import Trace


class CausalInconsistencyError(ValueError):
    """Raised when a trace contains causally inconsistent or invalid relationships."""
    pass


class CausalCycleError(CausalInconsistencyError):
    """Raised when a trace contains circular causal dependencies in strict mode."""
    pass


class AmbiguousMessageError(CausalInconsistencyError):
    """Raised when message correlation cannot unambiguously match send and receive."""
    pass


@dataclass
class GraphDiagnostics:
    """Diagnostic details regarding graph consistency, message matching, and data flow."""
    unmatched_messages: List[Dict[str, Any]] = field(default_factory=list)
    duplicate_messages: List[Dict[str, Any]] = field(default_factory=list)
    ambiguous_messages: List[Dict[str, Any]] = field(default_factory=list)
    unmatched_reads: List[Dict[str, Any]] = field(default_factory=list)
    cycle_detected: bool = False
    cycles: List[List[Tuple[str, str]]] = field(default_factory=list)
    conflicting_edges: List[Tuple[str, str]] = field(default_factory=list)
    repaired_edges: List[Tuple[str, str]] = field(default_factory=list)
    diagnosed_causes: List[str] = field(default_factory=list)

    def has_anomalies(self) -> bool:
        return bool(
            self.unmatched_messages
            or self.duplicate_messages
            or self.ambiguous_messages
            or self.unmatched_reads
            or self.cycle_detected
        )


class CausalGraph:
    """
    Directed Acyclic Graph (DAG) representing happens-before (->) relations:
    - Program order: consecutive events on the same node/thread
    - Message order: SEND(m) -> RECEIVE(m) with unique ID and correlation checks
    - Data flow: WRITE(store, k, v) -> READ(store, k, v) with version/tx isolation
    - Fault dependencies: FAULT -> impacted operations
    - Explicit dependencies: event.depends_on
    """

    def __init__(
        self,
        trace: Trace,
        strict_mode: bool = False,
        enable_cache: bool = True,
    ):
        self.trace = trace
        self.strict_mode = strict_mode
        self.enable_cache = enable_cache
        self.graph = nx.DiGraph()
        self.diagnostics = GraphDiagnostics()
        self.is_uncertain: bool = False
        self.uncertainty_reasons: List[str] = []
        self._ancestors_cache: Dict[str, Set[str]] = {}
        self._descendants_cache: Dict[str, Set[str]] = {}
        self._build_graph()

    def _clear_cache(self) -> None:
        self._ancestors_cache.clear()
        self._descendants_cache.clear()

    @staticmethod
    def _extract_store(event: Event) -> str:
        payload = event.payload
        store = (
            payload.get("store")
            or payload.get("store_id")
            or payload.get("database")
            or payload.get("db")
            or payload.get("replica")
            or payload.get("tenant")
            or "default"
        )
        return str(store)

    @staticmethod
    def _extract_key(event: Event) -> Optional[str]:
        payload = event.payload
        key = payload.get("key") if "key" in payload else payload.get("state_key")
        return str(key) if key is not None else None

    @staticmethod
    def _extract_version(event: Event) -> Optional[str]:
        payload = event.payload
        v = (
            payload.get("version")
            or payload.get("write_id")
            or payload.get("tx_id")
            or payload.get("tx")
        )
        return str(v) if v is not None else None

    @staticmethod
    def _extract_observed_version(event: Event) -> Optional[str]:
        payload = event.payload
        v = (
            payload.get("read_version")
            or payload.get("observed_version")
            or payload.get("version")
            or payload.get("write_id")
            or payload.get("tx_id")
        )
        return str(v) if v is not None else None

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

        # 3. Message order: SEND -> RECEIVE matching with identity validation & correlation
        message_sends: Dict[str, List[Event]] = {}
        for event in self.trace.events:
            if event.event_type == EventType.SEND:
                msg_id = event.payload.get("msg_id") or event.payload.get("message_id")
                if msg_id:
                    message_sends.setdefault(str(msg_id), []).append(event)

        # Report duplicate sends
        for m_id, send_list in message_sends.items():
            if len(send_list) > 1:
                self.diagnostics.duplicate_messages.append({
                    "msg_id": m_id,
                    "event_ids": [s.event_id for s in send_list],
                })
                self.is_uncertain = True
                self.uncertainty_reasons.append(
                    f"Duplicate message SEND detected for msg_id '{m_id}' (events: {[s.event_id for s in send_list]})"
                )

        for event in self.trace.events:
            if event.event_type == EventType.RECEIVE:
                msg_id = event.payload.get("msg_id") or event.payload.get("message_id")
                if not msg_id:
                    continue
                m_str = str(msg_id)
                if m_str not in message_sends:
                    self.diagnostics.unmatched_messages.append({
                        "receive_id": event.event_id,
                        "msg_id": m_str,
                        "node": event.node,
                    })
                    continue

                candidates = message_sends[m_str]
                matched_send: Optional[Event] = None

                if len(candidates) == 1:
                    matched_send = candidates[0]
                else:
                    # Multiple sends share the same msg_id: correlate via sequence, correlation_id, or endpoints
                    corr_id = event.payload.get("correlation_id")
                    seq = event.payload.get("seq")
                    sender = event.payload.get("sender") or event.payload.get("source")

                    for cand in candidates:
                        if corr_id and cand.payload.get("correlation_id") == corr_id:
                            matched_send = cand
                            break
                        if seq is not None and cand.payload.get("seq") == seq:
                            matched_send = cand
                            break
                        if sender and cand.node == sender:
                            matched_send = cand
                            break
                        if cand.target and cand.target == event.node:
                            matched_send = cand
                            break

                    if matched_send is None:
                        # Ambiguous: select candidate closest in trace order prior to receive
                        preceding = [c for c in candidates if self._trace_order(c.event_id) < self._trace_order(event.event_id)]
                        matched_send = preceding[-1] if preceding else candidates[0]
                        self.diagnostics.ambiguous_messages.append({
                            "receive_id": event.event_id,
                            "msg_id": m_str,
                            "candidates": [c.event_id for c in candidates],
                            "selected": matched_send.event_id,
                        })
                        if self.strict_mode:
                            raise AmbiguousMessageError(
                                f"Ambiguous message match for msg_id '{m_str}' at receive {event.event_id}. "
                                f"Multiple sends: {[c.event_id for c in candidates]}"
                            )
                        self.is_uncertain = True
                        self.uncertainty_reasons.append(
                            f"Ambiguous message match for msg_id '{m_str}' between receive {event.event_id} and sends {[c.event_id for c in candidates]}"
                        )

                if matched_send:
                    self.graph.add_edge(
                        matched_send.event_id,
                        event.event_id,
                        edge_type="message_order",
                        msg_id=m_str,
                    )

        # 4. Data flow (Read-After-Write) matching with store & version isolation
        writes_by_store_key: Dict[Tuple[str, str], List[Event]] = {}
        writes_by_version: Dict[Tuple[str, str, str], Event] = {}

        for event in self.trace.events:
            if event.event_type == EventType.WRITE:
                key = self._extract_key(event)
                if key is not None:
                    store = self._extract_store(event)
                    writes_by_store_key.setdefault((store, key), []).append(event)
                    ver = self._extract_version(event)
                    if ver:
                        writes_by_version[(store, key, ver)] = event

        for event in self.trace.events:
            if event.event_type == EventType.READ:
                key = self._extract_key(event)
                if key is None:
                    continue
                store = self._extract_store(event)
                obs_ver = self._extract_observed_version(event)
                obs_val = event.payload.get("val") if "val" in event.payload else event.payload.get("value")

                matched_write: Optional[Event] = None

                # 4A. Match by explicit version/tx metadata if present
                if obs_ver and (store, key, obs_ver) in writes_by_version:
                    matched_write = writes_by_version[(store, key, obs_ver)]

                # 4B. Match by store and key
                if matched_write is None and (store, key) in writes_by_store_key:
                    preceding_writes = [
                        w for w in writes_by_store_key[(store, key)]
                        if self._trace_order(w.event_id) < self._trace_order(event.event_id)
                    ]
                    if obs_val is not None:
                        # Find latest preceding write that produced obs_val
                        matching_val_writes = [
                            w for w in preceding_writes
                            if (w.payload.get("val") if "val" in w.payload else w.payload.get("value")) == obs_val
                        ]
                        if matching_val_writes:
                            matched_write = matching_val_writes[-1]
                        elif preceding_writes:
                            matched_write = preceding_writes[-1]
                    elif preceding_writes:
                        matched_write = preceding_writes[-1]

                # 4C. Fallback to default store if store was not explicitly found
                if matched_write is None and store != "default" and ("default", key) in writes_by_store_key:
                    preceding_writes = [
                        w for w in writes_by_store_key[("default", key)]
                        if self._trace_order(w.event_id) < self._trace_order(event.event_id)
                    ]
                    if preceding_writes:
                        matched_write = preceding_writes[-1]

                if matched_write:
                    self.graph.add_edge(
                        matched_write.event_id,
                        event.event_id,
                        edge_type="data_flow",
                        store=store,
                        key=key,
                    )
                else:
                    self.diagnostics.unmatched_reads.append({
                        "read_id": event.event_id,
                        "store": store,
                        "key": key,
                        "observed_val": obs_val,
                        "observed_version": obs_ver,
                    })

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
                for prev in self.trace.events:
                    if prev.event_id == fail_evt.event_id:
                        break
                    if prev.node == fail_evt.node:
                        self.graph.add_edge(prev.event_id, fail_evt.event_id, edge_type="assertion_dep")
                if self.graph.in_degree(self.trace.failure_event_id) == 0:
                    for prev in self.trace.events:
                        if prev.event_id == fail_evt.event_id:
                            break
                        if prev.is_state_access() or prev.is_fault():
                            self.graph.add_edge(prev.event_id, fail_evt.event_id, edge_type="assertion_dep")

        # 7. Cycle detection and resolution
        if not nx.is_directed_acyclic_graph(self.graph):
            self._handle_cycles()

    def _trace_order(self, event_id: str) -> int:
        for idx, e in enumerate(self.trace.events):
            if e.event_id == event_id:
                return idx
        return 999999

    def _diagnose_cycle(self, cycle: List[Tuple[str, str, Any]]) -> str:
        """Diagnose root cause of circular causality."""
        edge_types = set()
        for u, v, _ in cycle:
            data = self.graph.get_edge_data(u, v, {})
            edge_types.add(data.get("edge_type", "unknown"))

        if "vector_clock" in edge_types:
            return "Clock skew or clock inconsistency between distributed nodes"
        elif "explicit_dep" in edge_types:
            return "Cyclic explicit dependencies in trace input (malformed trace data)"
        elif "message_order" in edge_types and "program_order" in edge_types:
            return "Send-receive causal inversion or message ordering log anomaly (faulty instrumentation)"
        else:
            return "Inconsistent timestamps or concurrent instrumentation race condition"

    def _handle_cycles(self) -> None:
        """Detect and either reject or heuristically repair cycles, recording diagnostics."""
        trace_order = {e.event_id: i for i, e in enumerate(self.trace.events)}
        self.diagnostics.cycle_detected = True

        try:
            while not nx.is_directed_acyclic_graph(self.graph):
                cycle = nx.find_cycle(self.graph, orientation="original")
                cycle_edges = [(u, v) for u, v, _ in cycle]
                self.diagnostics.conflicting_edges.extend(cycle_edges)
                cause = self._diagnose_cycle(cycle)
                self.diagnostics.diagnosed_causes.append(cause)

                if self.strict_mode:
                    raise CausalCycleError(
                        f"Causal cycle detected in trace: {cycle_edges}. "
                        f"Likely cause: {cause}. Trace cannot be processed in strict mode."
                    )

                # Permissive repair mode: identify and remove worst offending edge
                worst_edge = None
                worst_diff = -float("inf")
                for u, v, _ in cycle:
                    diff = trace_order.get(u, 0) - trace_order.get(v, 0)
                    if diff > worst_diff:
                        worst_diff = diff
                        worst_edge = (u, v)

                if worst_edge and self.graph.has_edge(*worst_edge):
                    self.graph.remove_edge(*worst_edge)
                    self.diagnostics.repaired_edges.append(worst_edge)
                else:
                    u, v, _ = cycle[0]
                    self.graph.remove_edge(u, v)
                    self.diagnostics.repaired_edges.append((u, v))

                self.is_uncertain = True
                self.uncertainty_reasons.append(
                    f"Causal cycle detected and heuristically repaired: removed edge {worst_edge or (u, v)}. "
                    f"Conflicting cycle edges: {cycle_edges}. Likely cause: {cause}. Graph causality marked uncertain."
                )
        except CausalCycleError:
            raise
        except Exception as e:
            if self.strict_mode:
                raise CausalCycleError(f"Causal cycle error in trace: {str(e)}") from e

        self._clear_cache()

    def _get_ancestors(self, node_id: str) -> Set[str]:
        """Cached reachability ancestor query."""
        if not self.enable_cache:
            return nx.ancestors(self.graph, node_id) if self.graph.has_node(node_id) else set()
        if node_id not in self._ancestors_cache:
            if not self.graph.has_node(node_id):
                return set()
            self._ancestors_cache[node_id] = nx.ancestors(self.graph, node_id)
        return self._ancestors_cache[node_id]

    def causal_cone(self, target_event_id: str) -> Set[str]:
        """
        Backward slice: compute all events that causally precede target_event_id.
        C(target) = {e | e ->* target} U {target}.
        """
        if not self.graph.has_node(target_event_id):
            return set()
        ancestors = set(self._get_ancestors(target_event_id))
        ancestors.add(target_event_id)
        return ancestors

    def forward_cone(self, source_event_id: str) -> Set[str]:
        """
        Forward slice: compute all events causally influenced by source_event_id.
        F(source) = {e | source ->* e} U {source}.
        """
        if not self.graph.has_node(source_event_id):
            return set()
        if not self.enable_cache:
            desc = nx.descendants(self.graph, source_event_id)
        else:
            if source_event_id not in self._descendants_cache:
                self._descendants_cache[source_event_id] = nx.descendants(self.graph, source_event_id)
            desc = set(self._descendants_cache[source_event_id])
        desc.add(source_event_id)
        return desc

    def are_concurrent(self, event_id_a: str, event_id_b: str) -> bool:
        """
        Two events are concurrent iff neither has a causal path to the other.
        Optimized with O(1) cached reachability lookups.
        """
        if not self.graph.has_node(event_id_a) or not self.graph.has_node(event_id_b):
            return False
        if event_id_a == event_id_b:
            return False

        a_ancestors = self._get_ancestors(event_id_a)
        if event_id_b in a_ancestors:
            return False

        b_ancestors = self._get_ancestors(event_id_b)
        if event_id_a in b_ancestors:
            return False

        return True

    def find_conflicting_pairs(self) -> List[Tuple[Event, Event]]:
        """
        Identify pairs of concurrent events that access the same shared state store and key,
        where at least one is a WRITE (race condition candidates).
        Indexed by (store, key) for fast O(K * m^2) scaling rather than O(N^2).
        """
        conflicts = []
        store_key_map: Dict[Tuple[str, str], List[Event]] = {}

        for nid in self.graph.nodes:
            evt: Event = self.graph.nodes[nid]["event"]
            if evt.is_state_access():
                key = self._extract_key(evt)
                if key is not None:
                    store = self._extract_store(evt)
                    store_key_map.setdefault((store, key), []).append(evt)

        for (store, key), evts in store_key_map.items():
            for i in range(len(evts)):
                e1 = evts[i]
                for j in range(i + 1, len(evts)):
                    e2 = evts[j]
                    if e1.event_type == EventType.WRITE or e2.event_type == EventType.WRITE:
                        if self.are_concurrent(e1.event_id, e2.event_id):
                            conflicts.append((e1, e2))
        return conflicts

    def is_causally_closed(self, subset_ids: Set[str]) -> bool:
        """Check if a subset of event IDs is downward-closed under happens-before."""
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
            "is_uncertain": self.is_uncertain,
            "uncertainty_reasons": list(self.uncertainty_reasons),
            "diagnostics": {
                "unmatched_messages": len(self.diagnostics.unmatched_messages),
                "duplicate_messages": len(self.diagnostics.duplicate_messages),
                "ambiguous_messages": len(self.diagnostics.ambiguous_messages),
                "unmatched_reads": len(self.diagnostics.unmatched_reads),
                "cycle_detected": self.diagnostics.cycle_detected,
                "repaired_edges": len(self.diagnostics.repaired_edges),
            },
        }
