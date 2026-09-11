# Option C — Orchestrator + Distributed Locks + Event Log

**Status:** For multi-region, workflow-adjacent, 1M+ runs/sec schedulers
**Scale Target:** 10M schedules, 1B+ runs/day, 1M runs/sec peak, multi-region active-active
**Semantics:** Effectively-once with cooperative handlers; at-least-once transport

---

## 1. Problem Statement

A scheduler that must operate across regions, support workflow-adjacent semantics (DAG dependencies, child jobs), provide strong ordering guarantees per tenant, and scale to 1M+ runs/sec. The org already operates a Raft-based coordination service (etcd/ZooKeeper/Consul) and a durable log (Kafka).

**Non-goals:** general-purpose workflow engine (that's Temporal/Cadence territory) — but the design should not preclude adding DAG support later.

---

## 2. Requirements

Same as Option A, plus:
| ID | Requirement | Target |
|----|-------------|--------|
| N2'' | Firing latency (p99) | < 100ms |
| N3'' | Throughput | 1M runs/sec peak |
| N8 | Multi-region | Active-active, RPO=0 per region |
| N9 | Ordering | Per-tenant FIFO for ordered jobs |
| N10 | Dependencies | Job B runs after Job A succeeds (optional v2) |

---

## 3. Architecture

```
                         ┌───────────────────────────────┐
                         │  Coordination Service (Raft)  │
                         │  etcd / ZooKeeper / Consul    │
                         │  - leader election            │
                         │  - distributed locks          │
                         │  - cluster membership         │
                         └──────────────┬────────────────┘
                                        │  lease / lock / watch
       ┌────────────────────────────────┼────────────────────────────────┐
       │                                │                                │
       ▼                                ▼                                ▼
┌──────────────┐              ┌──────────────────┐              ┌──────────────┐
│  Region A    │              │  Region B        │              │  Region C    │
│  ┌────────┐  │              │  ┌────────┐      │              │  ┌────────┐  │
│  │Orches- │  │              │  │Orches- │      │              │  │Orches- │  │
│  │trator  │  │              │  │trator  │      │              │  │trator  │  │
│  │(leader)│  │              │  │(leader)│      │              │  │(leader)│  │
│  └───┬────┘  │              │  └───┬────┘      │              │  └───┬────┘  │
│      │       │              │      │           │              │      │       │
│  ┌───▼────┐  │              │  ┌───▼────┐      │              │  ┌───▼────┐  │
│  │Timing  │  │              │  │Timing  │      │              │  │Timing  │  │
│  │Wheel   │  │              │  │Wheel   │      │              │  │Wheel   │  │
│  │(shard) │  │              │  │(shard) │      │              │  │(shard) │  │
│  └───┬────┘  │              │  └───┬────┘      │              │  └───┬────┘  │
│      │       │              │      │           │              │      │       │
│  ┌───▼────┐  │              │  ┌───▼────┐      │              │  ┌───▼────┐  │
│  │Workers │  │              │  │Workers │      │              │  │Workers │  │
│  └───
