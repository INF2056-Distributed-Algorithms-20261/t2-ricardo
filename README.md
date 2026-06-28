# t2-ricardo — Boshu Election Strategy

Custom election strategy for the GrADySim UAV simulation. Based on the [t2-template](https://github.com/INF2056-Distributed-Algorithms-20261/t2-template), this implementation replaces the default Bully reconciliation rule with a **packet-count-aware** variant.

## What changed from the default

### The idea

In the default strategy (`BullyReconciliation`), the UAV with the **highest node ID** always wins the election. This means the same UAV tends to become leader regardless of how much data each node has collected.

The **Boshu strategy** (`PacketCountBullyReconciliation`) changes the reconciliation rule so that the UAV with the **most buffered packets** wins. If two UAVs have the same packet count, the **highest ID** breaks the tie, preserving the original Bully behaviour as a fallback.

### Changed files

#### [`src/strategies_boshu.py`](src/strategies_boshu.py) (new file)

Contains the three strategy classes for the Boshu variant. The anomaly detection and invitation strategies are identical to the defaults. The key difference is:

- **`PacketCountBullyReconciliation`** — replaces `BullyReconciliation`.
  - `choose_leader()` picks the candidate with the highest `(packet_count, id)` tuple instead of just `max(id)`.
  - `build_election_message()` includes a `packet_count` field in the election broadcast so that receiving nodes know the sender's buffer size.
  - `update_candidate_info()` — a new method called by the election mixin before `choose_leader()` to store the sender's packet count from the incoming election message. This is necessary because the standard `choose_leader(ctx, candidates)` signature only provides the local node's buffer via `ctx.packets`; the remote candidate's count must be passed in separately.

#### [`src/election_mixin.py`](src/election_mixin.py)

A small hook was added in `_on_receive_election()`: before calling `choose_leader()`, the mixin checks if the reconciliation strategy has an `update_candidate_info` method and, if so, calls it with the sender's raw election message. This is backward-compatible,  strategies that don't define this method (like the default `BullyReconciliation`) are unaffected.

```python
# In _on_receive_election():
if hasattr(self.reconciliation_strategy, "update_candidate_info"):
    self.reconciliation_strategy.update_candidate_info(sender_id, raw)
```

#### [`src/main.py`](src/main.py)

Updated to import and wire in the Boshu strategies for all UAVs:

```python
from src.strategies_boshu import (
    HeartbeatTimeoutDetection,
    AnyMemberLeadInvitation,
    PacketCountBullyReconciliation,
)

UAVClass = make_uav_viz(
    cluster_index=i,
    waypoints=LEFT_WAYPOINTS,
    group_size=n_left,
    loop=args.loop,
    anomaly_strategy=HeartbeatTimeoutDetection(),
    invitation_strategy=AnyMemberLeadInvitation(),
    reconciliation_strategy=PacketCountBullyReconciliation(),
)
```

### What did NOT change

- **Anomaly detection** — same heartbeat-timeout-based split/merge detection.
- **Invitation** — same any-member-starts-election model.
- **Collection mode** — still `FROM_ALL_MEMBERS` (every non-leader sends data to the leader).
- **Election message types** — same `election`, `alive`, `leader`, `data_transfer` protocol. The only addition is the `packet_count` field in the `election` message.

## Setup

```bash
python -m venv venv
# Windows
.\venv\Scripts\activate
# Linux / macOS
source venv/bin/activate

pip install -r requirements.txt
```

## Running

Always run from the **project root** using the `-m` flag so that `utils/` is on the path:

```bash
python -m src.main
```

### CLI flags

| Flag | Description |
|------|-------------|
| *(none)* | Run simulation with browser visualisation enabled; UAVs do a single start→end run |
| `--no-viz` | Disable the browser visualisation |
| `--force-regen` | Force regeneration of sensor positions and cluster waypoints |
| `--loop` | UAVs bounce back and forth along their path instead of stopping at the endpoint |

By default (`--loop` not set) the simulation **ends automatically** once every UAV has reached its final waypoint. With `--loop`, UAVs patrol continuously until `SIMULATION_DURATION` is reached.

The visualisation connects to the GrADySim web viewer at  
<https://project-gradys.github.io/gradys-sim-nextgen-visualization/>

> **Note (Windows):** The visualization handler spawns a subprocess for the WebSocket server.
> Any top-level code that should run only once must be inside `if __name__ == "__main__":`.
