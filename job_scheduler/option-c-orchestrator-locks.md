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
│  └───┬────┘  │              │  └───┬────┘      │              │  └───┬────┘  │
│      │       │              │      │           │              │      │       │
│  ┌───▼────┐  │              │  ┌───▼────┐      │              │  ┌───▼────┐  │
│  │Local   │  │              │  │Local   │      │              │  │Local   │  │
│  │State   │  │              │  │State   │      │              │  │State   │  │
│  │(RocksDB│  │              │  │(RocksDB│      │              │  │(RocksDB│  │
│  │+ WAL)  │  │              │  │+ WAL)  │      │              │  │+ WAL)  │  │
│  └───┬────┘  │              │  └───┬────┘      │              │  └───┬────┘  │
└──────┼───────┘              └──────┼───────────┘              └──────┼───────┘
       │                             │                                 │
       └──────────────┬──────────────┴──────────────┬──────────────────┘
                      │                             │
                      ▼                             ▼
           ┌────────────────────┐        ┌────────────────────┐
           │  Global Event Log  │        │  Global State DB   │
           │  (Kafka / Pulsar)  │        │  (CockroachDB /    │
           │  - run lifecycle   │        │   Spanner / Vitess)│
           │  - schedule changes│        │  - projections     │
           │  - replayable      │        │  - query API       │
           └────────────────────┘        └────────────────────┘

Ingestion path (per region):
  Client → Ingestion Svc → write to Global State DB + Event Log (via outbox)
                        → Orchestrator (leader) notified via watch

Timing path (per region, per shard):
  Orchestrator → Timing Wheel (in-memory, WAL-backed)
              → on fire: acquire distributed lock for (job_id, fire_time)
              → write run to Event Log + State DB
              → publish run.ready to regional broker
              → workers claim via lock + lease
```

**Key principles:**
1. **Orchestrator per region**, leader-elected via Raft. Owns the timing wheel and global view for its region.
2. **Distributed lock per (job_id, scheduled_fire_at)** prevents duplicate firing across regions.
3. **Event log is the durable backbone.** State DB is a projection that can be rebuilt from the log.
4. **Workers claim via lock + lease.** Lock for mutual exclusion; lease for crash recovery.
5. **Multi-region active-active** with regional orchestrators coordinating via the global lock service.

---

## 4. Data Model

### 4.1 Global State DB (CockroachDB / Spanner)

```sql
CREATE TABLE job_schedule (
  job_id          UUID PRIMARY KEY,
  tenant_id       UUID NOT NULL,
  region          TEXT NOT NULL,          -- home region
  name            TEXT NOT NULL,
  schedule_type   TEXT NOT NULL,
  cron_expr       TEXT,
  timezone        TEXT NOT NULL,
  payload         JSONB NOT NULL,
  priority        SMALLINT NOT NULL,
  max_attempts    SMALLINT NOT NULL,
  timeout_secs    INT NOT NULL,
  state           TEXT NOT NULL,
  next_run_at     TIMESTAMPTZ NOT NULL,
  version         BIGINT NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL,
  updated_at      TIMESTAMPTZ NOT NULL,
  -- CockroachDB/Spanner: geo-partition by region
) PARTITION BY LIST (region);

CREATE TABLE job_run (
  run_id            UUID PRIMARY KEY,
  job_id            UUID NOT NULL,
  tenant_id         UUID NOT NULL,
  region            TEXT NOT NULL,
  scheduled_fire_at TIMESTAMPTZ NOT NULL,
  status            TEXT NOT NULL,
  attempt           SMALLINT NOT NULL,
  lock_token        TEXT,               -- distributed lock holder
  lease_expires_at  TIMESTAMPTZ,
  payload           JSONB NOT NULL,
  result            JSONB,
  error             TEXT,
  created_at        TIMESTAMPTZ NOT NULL,
  UNIQUE (job_id, scheduled_fire_at)
) PARTITION BY LIST (region);
```

### 4.2 Event Log Schema (Kafka / Pulsar)

| Topic | Key | Retention | Purpose |
|-------|-----|-----------|---------|
| `schedule.created.v1` | `job_id` | ∞ (compacted) | Schedule definition |
| `schedule.updated.v1` | `job_id` | ∞ (compacted) | Schedule changes |
| `run.scheduled.v1` | `run_id` | 30d | Run created |
| `run.claimed.v1` | `run_id` | 30d | Worker claimed |
| `run.heartbeat.v1` | `run_id` | 1d | Lease renewal |
| `run.completed.v1` | `run_id` | ∞ | Terminal success |
| `run.failed.v1` | `run_id` | ∞ | Terminal failure |
| `run.retry.v1` | `run_id` | 30d | Retry scheduled |

**Event schema (Avro / Protobuf):**
```protobuf
message RunScheduled {
  string run_id = 1;
  string job_id = 2;
  string tenant_id = 3;
  string region = 4;
  google.protobuf.Timestamp scheduled_fire_at = 5;
  bytes payload = 6;
  int32 priority = 7;
  int32 max_attempts = 8;
  int64 event_time_ns = 9;   // for ordering
  string causation_id = 10;  // for tracing
}
```

### 4.3 Distributed Lock Schema (etcd)

```
Key:   /locks/runs/{run_id}
Value: {worker_id, region, acquired_at, lease_id}
Lease: TTL = timeout_secs + slack

Key:   /locks/fire/{job_id}/{scheduled_fire_at_unix_nano}
Value: {region, orchestrator_id, acquired_at}
Lease: TTL = 30s (short; only for firing decision)
```

### 4.4 Local State (RocksDB per orchestrator/worker)

- Timing wheel: `next_run_at → [job_id]` (in-memory, WAL-backed).
- Pending claims: `run_id → lease` (for fast heartbeat without DB round-trip).
- Rebuildable from Event Log on restart.

---

## 5. Critical Features & Approaches

### 5.1 Orchestrator (Raft Leader per Region)

**Responsibilities:**
- Own the timing wheel for its region's partitions.
- Elect leader via Raft (etcd). Followers hot standby with replicated state.
- Watch `schedule.*` events to update timing wheel.
- On timer fire: acquire distributed lock, emit `run.scheduled`, notify workers.
- Periodically checkpoint timing wheel to Event Log (for fast restart).

**Leader failover:**
- Raft elects new leader in ~1–2s.
- New leader replays Event Log from last checkpoint.
- In-flight locks held by old leader expire naturally.
- Workers continue claiming from State DB; no data loss.

### 5.2 Distributed Lock Protocol

**Why locks in addition to leases?**
- Leases are DB-row-based; locks are coordination-service-based.
- Locks provide **cross-region mutual exclusion** without cross-region DB writes.
- Locks give **fencing tokens** for split-brain protection.

**Firing lock (orchestrator-level):**
```
lock = etcd.acquire("/locks/fire/{job_id}/{fire_time}", ttl=30s)
if not lock.acquired:
    return  # another region already firing this job
try:
    emit run.scheduled
    write job_run to State DB
finally:
    lock.release()
```

**Execution lock (worker-level):**
```
lock = etcd.acquire("/locks/runs/{run_id}", ttl=lease_ttl)
if not lock.acquired:
    return  # another worker owns it
# Fencing token: monotonically increasing per lock acquisition
fencing_token = lock.fencing_token
try:
    execute(run_id, fencing_token)
    heartbeat(lock)
finally:
    lock.release()
```

**Fencing tokens** protect against stale workers: if a worker's lease expired and a new worker acquired the lock, the old worker's fencing token is stale. Downstream systems reject writes with stale tokens. This is the **Kleppmann fencing token pattern**.

### 5.3 Worker Claim & Execution

```
on run.ready (regional broker):
  lock = etcd.acquire("/locks/runs/{run_id}", ttl=lease_ttl)
  if not acquired: ack; return

  # Verify run still claimable in State DB (projection may lag)
  run = StateDB.get(run_id)
  if run.status != 'PENDING': lock.release(); ack; return

  # Update State DB with lock token
  StateDB.update(run_id, status='RUNNING', lock_token=lock.fencing_token)

  # Emit event
  EventLog.emit(run.claimed, {run_id, worker_id, fencing_token})

  # Execute with heartbeat
  try:
      result = execute(run.payload, timeout=run.timeout_secs,
                       fencing_token=lock.fencing_token)
      StateDB.update(run_id, status='SUCCEEDED', result=result)
      EventLog.emit(run.completed, {run_id, result})
  except Exception as e:
      StateDB.update(run_id, status='FAILED', error=str(e))
      EventLog.emit(run.failed, {run_id, error})
  finally:
      lock.release()
```

**Heartbeat:** `etcd.keep_alive(lock)` every `lease_ttl / 3`. If keep-alive fails, the worker must abort before side effects.

### 5.4 Event Log as Backbone

**Why an event log in addition to a DB?**
- **Replay:** State DB can be rebuilt from the log.
- **Cross-region replication:** Kafka MirrorMaker / Pulsar geo-replication.
- **Audit:** Immutable history of every state change.
- **Downstream consumers:** Analytics, alerting, billing subscribe to the log.
- **Decoupling:** Orchestrators, workers, and reconcilers communicate via events.

**State DB as projection:**
- Written by consumers of the event log (or via outbox from the same transaction).
- Can lag; must be reconciled.
- Read-optimized (indexes for query API).

**Consistency model:** Event log is append-only, ordered per key. State DB is eventually consistent (lag < 1s). Claims always verify against State DB; if State DB lags, claim fails and retries.

### 5.5 Multi-Region Active-Active

```
Region A: orchestrator A (leader for partitions 0-99), workers A
Region B: orchestrator B (leader for partitions 100-199), workers B
Region C: orchestrator C (leader for partitions 200-299), workers C

Global etcd cluster (5 nodes, spanning regions) for locks and leader election.
Global Kafka cluster (or per-region with MirrorMaker) for events.
Global State DB (CockroachDB/Spanner) with geo-partitioning.
```

**Failure scenarios:**
| Failure | Behavior |
|---------|----------|
| Region A down | Locks held by A expire; orchestrator B/C take over A's partitions via Raft re-election |
| etcd quorum lost | Scheduler pauses (fail-safe); no duplicate firing |
| Kafka region partition | Regional orchestrators buffer; replay on recovery |
| State DB region down | Local state (RocksDB) continues; writes queue; reconcile on recovery |
| Network partition A↔B | Both regions may try to fire same job; distributed lock prevents duplicate |

**RPO/RTO:**
- RPO = 0 (event log is durable, replicated)
- RTO < 30s (Raft election + replay)

### 5.6 Ordering Guarantees

- **Per-tenant FIFO:** All of a tenant's jobs keyed to same Kafka partition. Workers process in order.
- **Per-job FIFO:** Same `job_id` → same partition.
- **Cross-job ordering (DAG):** Job B has `depends_on: [A]`. Orchestrator only schedules B after A's `run.completed` event. This is the workflow-adjacent feature.

### 5.7 Multi-Tenancy

- **Geo-partitioning:** Tenant data pinned to home region (data residency).
- **Quotas:** Per-tenant token buckets in Redis, enforced at ingestion and claim.
- **Fairness:** Weighted fair queue at orchestrator; per-tenant worker pools.
- **Isolation:** Noisy tenant can be moved to dedicated partition set.

### 5.8 Observability

- **Distributed tracing:** OpenTelemetry, one trace per `run_id`, spans across regions.
- **Metrics:** `orchestrator_leader_changes`, `lock_acquire_latency`, `lock_contention_total`, `event_log_lag`, `state_db_projection_lag`, `fencing_token_rejections`.
- **Audit:** Every state change is an event; query the log for compliance.

### 5.9 Failure Modes

| Failure | Mitigation |
|---------|------------|
| Orchestrator leader crash | Raft re-election; replay from checkpoint |
| Split-brain (two leaders) | Raft quorum prevents; fencing tokens protect downstream |
| etcd quorum loss | Scheduler pauses; no duplicate firing (fail-safe) |
| Worker crash | Lease expires; lock released; reaper requeues |
| Stale worker after lease expiry | Fencing token rejected by downstream |
| Event log loss | Replicated; RPO=0 |
| State DB projection lag | Claims verify against DB; retry on lag |
| Cross-region network partition | Locks prevent duplicate; events replicate on heal |
| Poison job | max_attempts → DEAD; DLQ topic |
| Clock skew | Use logical timestamps (HLC) + DB time |

### 5.10 Scaling

| Dimension | Strategy |
|-----------|----------|
| Orchestrators | Add regions; add shards per region |
| Workers | Horizontal per region; partitioned consumer groups |
| etcd | 5-node cluster; add observers for read scale |
| Kafka | Add partitions/brokers; geo-replicate |
| State DB | Geo-partition; add nodes; read replicas |
| Local state | RocksDB per orchestrator; sharded by partition |

---

## 6. Tradeoffs

| Decision | Alternative | Why |
|----------|-------------|-----|
| Orchestrator + Raft | Sharded scanners | Sub-second latency; global view; multi-region |
| Distributed locks | DB leases only | Cross-region mutual exclusion; fencing tokens |
| Event log as backbone | DB only | Replay, audit, cross-region, downstream consumers |
| State DB as projection | State DB as truth | Scalability; log is the durable source |
| HLC timestamps | Wall clock | Ordering without clock sync |
| Geo-partitioning | Single-region | Data residency; latency; fault isolation |
| Fencing tokens | Lease-only | Protects downstream from stale workers |
| Orchestrator owns timing wheel | Polling scanners | p99 < 100ms |

**When NOT to use Option C:**
- Throughput < 50K runs/sec (Options A/B are simpler).
- Single region (coordination overhead not justified).
- Team lacks Raft/etcd/Kafka operational expertise.
- No workflow/DAG requirements.
- Small team — the operational surface is large.

**This is over-engineering for most cases.** Reach for it only when the requirements genuinely demand it.

---

## 7. Interviewer Deep-Dive Questions

**Q1: Why an orchestrator instead of sharded scanners?**
Sub-second latency (in-memory timing wheel vs. DB polling), global view for cross-shard ordering, and a natural home for leader election and DAG dependencies.

**Q2: Why a distributed lock service if you already have leases?**
Cross-region mutual exclusion without cross-region DB writes, and fencing tokens for split-brain protection. Leases are row-scoped; locks are coordination-scoped.

**Q3: What's a fencing token and why do I need it?**
A monotonically increasing number issued on each lock acquisition. Stale workers (whose lease expired) present an old token; downstream systems reject it. Prevents the "zombie worker" problem in Kleppmann's fencing token pattern.

**Q4: Two regions try to fire the same job. What happens?**
Both call `etcd.acquire("/locks/fire/{job_id}/{fire_time}")`. Only one wins. Loser returns without firing. Event log records the winner for audit.

**Q5: etcd quorum lost. What happens?**
Scheduler pauses (fail-safe). No new locks can be acquired → no duplicate firing. Workers finish in-flight jobs. On quorum restore, orchestrators resume.

**Q6: Orchestrator leader crashes mid-timing-wheel. What happens?**
Raft elects new leader in ~1–2s. New leader replays Event Log from last checkpoint (checkpoint every 10s). In-flight locks held by old leader expire. Workers continue claiming from State DB.

**Q7: How do you prevent the state DB from diverging from the event log?**
State DB is written by consumers of the event log (or via transactional outbox from the same transaction). A reconciler compares log offsets vs. DB versions and repairs gaps. Claims always verify against DB, so divergence only causes retries, not duplicates.

**Q8: How do you handle a slow consumer of the event log?**
Kafka consumer groups scale horizontally. Lag is monitored. Critical consumers (State DB projection) have dedicated groups with priority. Non-critical (analytics) can lag for hours.

**Q9: How do you do exactly-once here?**
You don't, at the transport level. You get **effectively-once** via: idempotent handlers, fencing tokens on downstream writes, and an effect ledger with unique keys. The event log + fencing token + ledger combination is the strongest practical guarantee.

**Q10: How do you handle ordering across regions?**
Hybrid Logical Clocks (HLC) in event timestamps. Events are totally ordered per key by (HLC, region_id). Cross-key ordering is best-effort; strict cross-key ordering requires a global sequencer (e.g., Spanner's TrueTime), which is expensive.

**Q11: What's the blast radius of a bad deploy?**
Per-region. Rolling deploy region-by-region. Orchestrator leader stays on old version until new version is healthy. Workers drain before termination.

**Q12: How do you test this?**
Chaos engineering: kill orchestrator leaders, partition regions, drop etcd quorum, inject clock skew, replay event logs, verify fencing tokens. Jepsen-style consistency tests for the lock service.

**Q13: Cost of running this?**
Significant: multi-region etcd, Kafka, CockroachDB/Spanner, plus orchestrator/worker fleets. Justify only at 1M+ runs/sec or multi-region requirements.

**Q14: How do you onboard a new region?**
Deploy orchestrator + workers + local RocksDB. Register in etcd. Rebalance partitions via consistent hashing. Replay relevant event log slice. Cut over traffic.

**Q15: What if a worker's downstream call doesn't support fencing tokens?**
Wrap it: worker writes to an outbox with the fencing token, and a relay rejects stale tokens before calling downstream. Or use an idempotency key derived from `(run_id, fencing_token)`.

**Q16: How do you handle DAG dependencies?**
Orchestrator subscribes to `run.completed` events. When a dependency completes, it schedules the dependent job's `run.scheduled` event. Cycles detected at ingestion. This is the workflow-adjacent feature.

**Q17: Why not just use Temporal?**
Temporal is excellent for workflow orchestration. For pure time-based scheduling at 1M+ runs/sec with multi-region, Temporal's workflow-centric model is heavier than needed. But if your requirements are mostly workflow, use Temporal.

**Q18: What's the single biggest risk?**
Operational complexity. Raft, etcd, Kafka, multi-region DB, fencing tokens — each is a failure domain. Mitigate with runbooks, chaos testing, and staged rollouts. The second risk is clock skew — mitigate with HLC.

**Q19: How do you migrate from Option A to Option C?**
Strangler pattern: run both in parallel. Option A handles existing jobs; Option C handles new jobs. Migrate tenants one at a time. Dual-write during migration; reconcile. Cut over when metrics match.

**Q20: How do you handle a tenant that suddenly submits 10M jobs?**
Ingestion rate-limits per tenant. Orchestrator's timing wheel is per-tenant quota'd. If a tenant exceeds quota, its jobs queue (not dropped). Alert on quota breaches.
