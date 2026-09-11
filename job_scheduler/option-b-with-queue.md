# Option B — Table + Notification Queue (Hybrid)

**Status:** For high-throughput deployments with a durable broker already present
**Scale Target:** 1M schedules, 100M runs/day, 100K runs/sec peak
**Semantics:** At-least-once; broker is a hint, table is source of truth

---

## 1. Problem Statement

Same functional scope as Option A, but with push latency requirements (p99 < 200ms) and higher throughput (~100K runs/sec). A durable broker (Kafka or Redis Streams) is already operated by the org.

**Non-goals:** exactly-once; DAG orchestration.

---

## 2. Requirements

Same as Option A, plus:
| ID | Requirement | Target |
|----|-------------|--------|
| N2' | Firing latency (p99) | < 200ms |
| N3' | Throughput | 100K runs/sec peak |

---

## 3. Architecture

```
┌──────────────┐
│  Clients     │
└──────┬───────┘
       │ gRPC / HTTP
       ▼
┌──────────────────────┐
│  Ingestion Service   │  validate → assign job_id, partition_id, next_run_at
│  (stateless, N pods) │  INSERT job_schedule (transactional outbox)
└──────┬───────────────┘
       │  publish schedule.created
       ▼
┌──────────────────────────────────────────────────────────┐
│  Event Broker (Kafka / Redis Streams)                    │
│  topics: schedule.created | schedule.due | run.claimed   │
│          run.completed    | run.failed  | run.retry      │
└──────┬───────────────────────────────────────────────────┘
       │  consume schedule.due
       ▼
┌──────────────────────────────────────────────────────────┐
│  Scheduler Fleet  (sharded by partition_id)              │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐          │
│  │ Shard 0    │  │ Shard 1    │  │ Shard K    │  ...     │
│  └─────┬──────┘  └─────┬──────┘  └─────┬──────┘          │
│        │ INSERT job_run (PENDING)                        │
│        │ publish run.ready (key = partition_id)          │
└────────┼─────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────┐
│  job_run table  (SOURCE OF TRUTH, time-partitioned)      │
│  - hot: PENDING / RUNNING    - cold: terminal → history  │
└────────┬─────────────────────────────────────────────────┘
         │  claim via conditional UPDATE + lease
         │  (consumer of run.ready; also polls on fallback)
         ▼
┌──────────────────────────────────────────────────────────┐
│  Worker Fleet  (consumer groups per partition)           │
│  - claim → heartbeat → execute → complete/fail           │
│  - publish run.completed / run.failed                    │
└──────────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────┐
│  Reaper / Reconciler                                     │
│  - scans job_run for expired leases                      │
│  - publishes run.retry                                   │
│  - reconciles broker vs. table divergence                │
└──────────────────────────────────────────────────────────┘
```

**Key principle:** Broker is a *notification bus*, not a queue of work. Work is always claimed from the table. Broker messages are hints that reduce latency.

---

## 4. Data Model

Same as Option A, plus:

### 4.1 Outbox table (for transactional publish)

```sql
CREATE TABLE outbox (
  id            BIGSERIAL PRIMARY KEY,
  aggregate_id  UUID NOT NULL,
  topic         TEXT NOT NULL,
  payload       JSONB NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at  TIMESTAMPTZ
);

CREATE INDEX idx_outbox_unpublished
  ON outbox (id) WHERE published_at IS NULL;
```

A relay process reads `outbox` and publishes to Kafka/Redis, then marks `published_at`. This guarantees "DB commit ⇒ event published" without 2PC.

### 4.2 Broker topics

| Topic | Key | Partitions | Retention | Purpose |
|-------|-----|------------|-----------|---------|
| `schedule.created` | `partition_id` | N | 7d | New schedule registered |
| `schedule.due` | `partition_id` | N | 7d | Schedule has fired; scan trigger |
| `run.ready` | `partition_id` | N | 1h | New PENDING run; worker wake-up |
| `run.claimed` | `run_id` | N | 1h | Claim audit |
| `run.completed` | `run_id` | N | 7d | Terminal state |
| `run.failed` | `run_id` | N | 7d | Terminal state |
| `run.retry` | `partition_id` | N | 1h | Reaper requeue signal |

---

## 5. Critical Features & Approaches

### 5.1 Ingestion (with Outbox)

```
BEGIN;
  INSERT INTO job_schedule (...);
  INSERT INTO outbox (aggregate_id, topic, payload)
    VALUES (job_id, 'schedule.created', {...});
COMMIT;
```

Relay publishes asynchronously. At-least-once publish is fine — consumers dedupe on `job_id` or are idempotent.

### 5.2 Scheduler Fleet

Two modes:

**(a) Timer-based (primary):** Scheduler maintains an in-memory timer wheel / priority queue of upcoming `next_run_at` values. On firing, it inserts `job_run` and publishes `run.ready`. This is the low-latency path.

**(b) Scan-based (safety net):** Periodic scan of `job_schedule WHERE next_run_at <= now()` catches anything the timer missed (e.g., after a restart). Same transaction as Option A.

```
on startup:
  load upcoming schedules for partition into timer wheel
  start scan loop (every 30s) as safety net

on timer fire:
  BEGIN;
    INSERT job_run ... ON CONFLICT DO NOTHING;
    UPDATE job_schedule SET next_run_at = ...;
    INSERT outbox (topic='run.ready', ...);
  COMMIT;
```

### 5.3 Worker Claim Protocol

Workers subscribe to `run.ready` (keyed by `partition_id`) via consumer groups. Each group member gets a subset of partitions. On message:

```
on run.ready:
  claimed = try_claim_from_table()   # same conditional UPDATE as Option A
  if claimed: execute(claimed)
  else: ack and move on              # someone else got it, or already done
```

Polling fallback: every 5s, workers also scan for PENDING runs to catch missed events.

**Why claim from the table, not from the message?**
- Table has priority, quotas, cancellation state.
- Broker message may be stale (already claimed, cancelled, or completed).
- Broker ordering/redelivery is not a claim mechanism.

### 5.4 Reaper / Reconciler

Two responsibilities:

1. **Lease reaper:** scans `job_run WHERE status='RUNNING' AND lease_expires_at < now()`, resets to PENDING, publishes `run.retry`.
2. **Divergence reconciler:** compares "broker says work exists" vs. "table has PENDING rows." If table has PENDING rows older than X with no broker activity, republish `run.ready`. This heals after broker outages.

### 5.5 Multi-Tenancy

Same as Option A. Broker partitions keyed by `partition_id`, so tenant isolation follows partition isolation.

### 5.6 Observability

Add:
- `broker_publish_lag_seconds`
- `broker_consume_lag_seconds` (per consumer group)
- `outbox_unpublished_count`
- `reconciler_republished_total`

### 5.7 Failure Modes

| Failure | Mitigation |
|---------|------------|
| Broker down | Workers fall back to polling; outbox accumulates; replay on recovery |
| Broker duplicates | Idempotent claim; `UNIQUE` on run insert |
| Broker loses messages | Reconciler republishes from table |
| Outbox relay crashes | Restart; idempotent publish via `published_at` |
| Consumer group rebalance | Workers stop claiming during rebalance; leases protect in-flight |
| DB down | Ingestion 503; broker holds events; replay on recovery |
| Worker crash | Lease expiry → reaper → `run.retry` |
| Thundering herd on `run.ready` | Jittered consume; batch claim |
| Poison message | DLQ per topic; alert |

### 5.8 Scaling

| Dimension | Strategy |
|-----------|----------|
| Ingestion | Horizontal; outbox relay scales independently |
| Scheduler | Shard by `partition_id`; timer wheel per shard |
| Workers | Consumer groups per partition; scale group size |
| Broker | Increase partitions; add brokers |
| DB | Same as Option A + read replicas for query API |

---

## 6. Tradeoffs

| Decision | Alternative | Why |
|----------|-------------|-----|
| Broker as hint, table as truth | Broker as truth | Avoids dual source of truth |
| Outbox for publish | 2PC | Avoids distributed transaction |
| Timer wheel + scan safety net | Scan only | Sub-second latency |
| Claim from table on message | Claim from message | Priority, quotas, cancellation |
| Consumer groups per partition | Single consumer | Linear scale, ordered per partition |

**When NOT to use Option B:**
- No broker already in the org (adding one is not worth it below ~50K runs/sec).
- Latency SLA ≥ 1s (Option A is simpler).
- Team lacks Kafka/Redis operational expertise.

---

## 7. Interviewer Deep-Dive Questions

**Q1: Why a broker if the table is source of truth?**
Latency. Push vs. pull. At 100K runs/sec, polling the DB is wasteful. Broker notifies workers; workers still claim from the table.

**Q2: What if broker delivers a `run.ready` for an already-completed run?**
`try_claim_from_table()` returns 0 rows (status != PENDING). Worker acks and moves on. Idempotent.

**Q3: What if broker loses a `run.ready`?**
Reconciler scans for PENDING runs older than threshold and republishes. Polling fallback also catches it.

**Q4: How do you publish atomically with the DB write?**
Transactional outbox. Same DB transaction inserts the row and the outbox event. Relay publishes asynchronously.

**Q5: Consumer group rebalance during execution — what happens?**
Rebalance stops new consumption. In-flight jobs keep their leases. If a worker is killed by rebalance, its lease expires → reaper requeues. Other workers pick up.

**Q6: Two workers in the same consumer group get the same message?**
At-least-once delivery means yes, possible. Both call `try_claim_from_table()`. One wins. Loser acks.

**Q7: How is this different from using Kafka as the job queue?**
Kafka has no priority, no per-tenant quotas, no "what's running" query, no cancellation, no arbitrary `next_run_at`. You'd rebuild all of that on top of Kafka — badly. The table gives it to you for free.

**Q8: What's the broker retention policy?**
Short (1h for `run.ready`, 7d for terminal events). The table is durable; broker is a hint. If the broker retains too long, you're paying for a second source of truth.

**Q9: How do you handle a broker partition hot spot?**
Key by `partition_id`, not `run_id`, so load spreads. If a single partition is hot, split it (consistent hashing) and reassign workers.

**Q10: What about exactly-once?**
Same as Option A: at-least-once + idempotent handlers + effect ledger.

**Q11: How do you test the reconciler?**
Chaos: kill broker for 5 min, verify all PENDING runs still fire. Kill workers mid-lease, verify reaper requeues.

**Q12: Cost of running Kafka + DB?**
Significant. Justify only above ~50K runs/sec or when sub-second latency is a hard requirement.

**Q13: What if the outbox relay is slow?**
Publish lag grows. Alert on `outbox_unpublished_count`. Scale relay horizontally (partitioned by `aggregate_id`).

**Q14: How do you order events per job?**
Key broker messages by `job_id` (or `partition_id` for scan-level ordering). Kafka guarantees order within a partition.
