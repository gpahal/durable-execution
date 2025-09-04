# Design diagram

## Task execution

The following diagram shows the complete task execution lifecycle with automatic and manual
transitions.

### Legend

- 🤖 **Automatic**: System-triggered transitions
- 👤 **Manual**: User-triggered transitions
- ⚡ **Recovery**: Process failure recovery transitions

### Diagram

```mermaid
flowchart TD
  A[Enqueue task] -->|🤖| B[status=ready<br/>closeStatus=idle]
  B -->|🤖 Executor picks up task| C[status=running<br/>expiresAt set]

  C -->|🤖 run function failed| D[status=failed]
  C -->|🤖 run function timed out| E[status=timed_out]
  C -->|🤖 run function completed| F{Did task return children?}
  C -->|👤 User cancellation| H[status=cancelled]
  C -->|⚡ Process failure + timeout| I[status=ready<br/>retry attempt]

  F -->|🤖 Yes| G[status=waiting_for_children<br/>children spawned]
  F -->|🤖 No| J{Does task have finalize?}

  G -->|🤖 All children finished| J
  G -->|👤 User cancellation| H
  G -->|🤖 Child failure propagation| D

  J -->|🤖 Yes| K[status=waiting_for_finalize<br/>finalize task spawned]
  J -->|🤖 No| L[status=completed]

  K -->|🤖 finalize failed| M[status=finalize_failed]
  K -->|🤖 finalize completed| L
  K -->|👤 User cancellation| H

  %% Cleanup transitions (all automatic)
  D -->|🤖 Background cleanup| Y[closeStatus=closing]
  E -->|🤖 Background cleanup| Y
  H -->|🤖 Background cleanup| Y
  L -->|🤖 Background cleanup| Y
  M -->|🤖 Background cleanup| Y
  I -->|🤖 Retry logic| B

  Y -->|🤖 Cleanup complete| Z[closeStatus=closed<br/>FINAL STATE]

  %% Style finished states
  classDef finished fill:#e1f5fe
  classDef error fill:#ffebee
  classDef processing fill:#f3e5f5

  class L finished
  class D,E,M,H error
  class C,G,K processing
```
