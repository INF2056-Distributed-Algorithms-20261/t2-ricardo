"""
Boshu election strategy implementations.

These are identical to the default **Bully algorithm** strategies except
for the reconciliation rule:

- :class:`HeartbeatTimeoutDetection` — split/merge via heartbeat timeout
- :class:`AnyMemberLeadInvitation` — any member that detects an anomaly
  starts the election
- :class:`PacketCountBullyReconciliation` — highest *packet count* wins;
  ties are broken by highest ID.  All non-leaders send their data to the
  leader.

The election message includes the sender's current packet count so that
receiving nodes can make an informed pairwise comparison.
"""
from __future__ import annotations

from typing import Dict, Set

from src.strategies import (
    AnomalyDetectionStrategy,
    AnomalyResult,
    CollectionMode,
    ElectionContext,
    InvitationStrategy,
    ReconciliationStrategy,
)

# Re-use the same timeout constant the mixin uses by default.
# Strategies that need a different threshold can define their own.
_DEFAULT_PEER_TIMEOUT = 5.0


# ── Anomaly Detection ─────────────────────────────────────────────────────


class HeartbeatTimeoutDetection(AnomalyDetectionStrategy):
    """Detect splits via heartbeat timeout (member → lead interaction).

    A peer whose last heartbeat is older than ``peer_timeout`` seconds
    is considered out of range.  If the lost peer was the current leader,
    a ``"split"`` trigger is returned; otherwise the lost peers are
    reported but no election trigger is raised.

    Merge detection is handled separately by the mixin when a heartbeat
    from a previously-unknown peer arrives (see
    :pymethod:`ElectionMixin.handle_packet`).
    """

    def __init__(self, peer_timeout: float = _DEFAULT_PEER_TIMEOUT) -> None:
        self.peer_timeout = peer_timeout

    def check_for_anomaly(self, ctx: ElectionContext) -> AnomalyResult:
        lost: Set[int] = set()
        for peer_id, last_seen in ctx.peer_last_seen.items():
            if ctx.current_time - last_seen > self.peer_timeout:
                lost.add(peer_id)

        if not lost:
            return AnomalyResult()

        trigger = None
        if ctx.current_leader_id in lost:
            trigger = "split"

        return AnomalyResult(lost_peers=lost, trigger=trigger)


# ── Invitation ────────────────────────────────────────────────────────────


class AnyMemberLeadInvitation(InvitationStrategy):
    """Any member that detects an anomaly immediately starts an election.

    This is the simplest invitation model: whoever notices the problem
    broadcasts an election message.  There is no lead-to-lead negotiation.
    """

    def should_start_election(
        self, ctx: ElectionContext, anomaly: AnomalyResult,
    ) -> bool:
        # Start an election whenever there is a trigger, regardless of role
        return anomaly.trigger is not None

    def build_election_message(self, ctx: ElectionContext) -> dict:
        total_packets = sum(ctx.packets.values()) + sum(ctx.election_buffer.values())
        return {
            "msg_type": "election",
            "sender_type": "uav",
            "sender_id": ctx.my_id,
            "packet_count": total_packets,
        }


# ── Reconciliation ────────────────────────────────────────────────────────


class PacketCountBullyReconciliation(ReconciliationStrategy):
    """Highest *packet count* wins; ties broken by highest ID.

    Every non-leader sends all data to the leader
    (``FROM_ALL_MEMBERS`` collection).

    This strategy keeps a transient map of known candidate packet counts
    so that pairwise comparisons in the Bully algorithm can consider
    buffer sizes.  The mixin calls :meth:`update_candidate_info` with
    the raw election message before invoking :meth:`choose_leader`.
    """

    def __init__(self) -> None:
        # candidate_id → total packet count (from election messages)
        self._peer_packet_counts: Dict[int, int] = {}

    def update_candidate_info(self, sender_id: int, raw_message: dict) -> None:
        """Store the sender's packet count from an incoming election message.

        Called by the election mixin before :meth:`choose_leader` so that
        the pairwise comparison has access to the remote node's buffer size.
        """
        pkt_count = raw_message.get("packet_count", 0)
        self._peer_packet_counts[sender_id] = pkt_count

    def choose_leader(self, ctx: ElectionContext, candidates: Set[int]) -> int:
        """Pick the candidate with the most packets; break ties by highest ID."""
        my_total = sum(ctx.packets.values()) + sum(ctx.election_buffer.values())

        def _priority(cid: int) -> tuple:
            if cid == ctx.my_id:
                return (my_total, cid)
            return (self._peer_packet_counts.get(cid, 0), cid)

        return max(candidates, key=_priority)

    def get_collection_mode(self) -> CollectionMode:
        return CollectionMode.FROM_ALL_MEMBERS
