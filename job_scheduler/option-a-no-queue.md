# Option A — Table-as-Source-of-Truth (No Queue)

**Status:** Recommended default
**Scale Target:** 100K schedules, 1M runs/day, 10K runs/sec peak
**Semantics:** At-least-once with idempotency contract

---

## 1. Problem Statement

Distributed job scheduler supporting one-shot, recurring (cron/interval), delayed, and priority jobs. Multi-tenant, at-least-once execution, horizontally scalable to ~10K runs/sec.

**Non-goals:** exactly-once execution; workflow DAG orchestration; sub-second precision.

---

## 2. Requirements

### Functional
| ID | Requirement |
|----|-------------|
| F1 | Ingest job definitions with validation; assign `job_id`, `partition_id`, `next_run_at` |
| F2 | Fire one-shot and recurring jobs at scheduled time |
| F3 | Retries with exponential backoff and max-attempt caps |
| F4 | Cancellation of pending/running jobs |
| F5 | Priority and per-tenant concurrency limits |
| F6 | Query run history and current state |

### Non-Functional
| ID | Requirement | Target |
|----|-------------|--------|
| N1 | Availability | 99.95% ingestion, 99.9% firing |
| N2 | Firing latency (p99) | < 2s |
| N3 | Throughput | 10K runs/sec peak |
| N4 | Durability | No lost jobs after ACK |
| N5 | Execution | At-least-once |
| N6 | Multi-tenancy | Noisy-neighbor isolation |
| N7 | Observability | Metrics, traces, logs per `run_id` |

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
│  (stateless, N pods) │  INSERT job_schedule
└──────┬───────────────┘
       │  NOTIFY scheduler:partition_N  (hint only)
       ▼
┌──────────────────────────────────────────────────────────┐
│  Scheduler Fleet  (sharded by partition_id)              │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐          │
│  │ Shard 0    │  │ Shard 1    │  │ Shard K    │  ...     │
│  │ (leader)   │  │ (leader)   │  │ (leader)   │          │
│  └─────┬──────┘  └─────┬──────┘  └─────┬──────┘          │
│        │ scan due schedules → INSERT job_run (PENDING)   │
│        │ NOTIFY workers:partition_N                      │
└────────┼─────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────┐
│  job_run table  (source of truth, time-partitioned)      │
│  - hot partition: PENDING / RUNNING                      │
│  - cold partitions: terminal states → history            │
└────────┬─────────────────────────────────────────────────┘
         │  claim via conditional UPDATE + lease
         ▼
┌──────────────────────────────────────────────────────────┐
│  Worker Fleet  (subscribed to workers:partition_N)       │
│  - claim → heartbeat lease → execute → complete/fail     │
│  - idempotent handlers keyed on job_run_id               │
└──────────────────────────────────────────────────────────┘
```

**Key principle:** DB is the source of truth. Notifications are a latency hint only.

---

## 4. Data Model

### 4.1 `job_schedule`

```sql
CREATE TABLE job_schedule (
  job_id            UUID PRIMARY KEY,
  tenant_id         UUID NOT NULL,
  partition_id      INT  NOT NULL,
  name              TEXT NOT NULL,
  schedule_type     TEXT NOT NULL,          -- 'ONCE' | 'CRON' | 'INTERVAL'
  cron_expr         TEXT,
  interval_secs     BIGINT,
  run_at            TIMESTAMPTZ,
  timezone          TEXT NOT NULL DEFAULT 'UTC',
  payload           JSONB NOT NULL,
  priority          SMALLINT NOT NULL DEFAULT 5,
  max_attempts      SMALLINT NOT NULL DEFAULT 3,
  timeout_secs      INT NOT NULL DEFAULT 300,
  state             TEXT NOT NULL,          -- 'ACTIVE' | 'PAUSED' | 'CANCELLED'
  next_run_at       TIMESTAMPTZ NOT NULL,
  last_run_at       TIMESTAMPTZ,
  version           BIGINT NOT NULL DEFAULT 0,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_schedule_due
  ON job_schedule (partition_id, next_run_at)
  WHERE state = 'ACTIVE';
```

### 4.2 `job_run`

```sql
CREATE TABLE job_run (
  run_id            UUID PRIMARY KEY,
  job_id            UUID NOT NULL,
  tenant_id         UUID NOT NULL,
  partition_id      INT  NOT NULL,
  scheduled_fire_at TIMESTAMPTZ NOT NULL,
  next_run_at       TIMESTAMPTZ NOT NULL,
  status            TEXT NOT NULL,          -- PENDING|RUNNING|SUCCEEDED|FAILED|DEAD
  attempt           SMALLINT NOT NULL DEFAULT 1,
  max_attempts      SMALLINT NOT NULL,
  priority          SMALLINT NOT NULL,
  lease_owner       TEXT,
  lease_expires_at  TIMESTAMPTZ,
  payload           JSONB NOT NULL,
  result            JSONB,
  error             TEXT,
  started_at        TIMESTAMPTZ,
  finished_at       TIMESTAMPTZ,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (job_id, scheduled_fire_at)
);

CREATE INDEX idx_run_claimable
  ON job_run (partition_id, priority, next_run_at)
  WHERE status = 'PENDING';

CREATE INDEX idx_run_lease
  ON job_run (lease_expires_at)
  WHERE status = 'RUNNING';
```

**Why these choices:**
- `UNIQUE (job_id, scheduled_fire_at)` — idempotency guard for scheduler retries.
- Partial indexes keep the working set small.
- `job_run` is time-partitioned weekly/monthly.

### 4.3 `job_run_history`

Terminal rows older than N days archived to a `job_run_history` table partitioned by month, or to Parquet on S3.

---

## 5. Critical Features & Approaches

### 5.1 Ingestion

Stateless gRPC. Validate payload, compute next fire time (timezone-aware cron), `partition_id = hash(tenant_id) % N`, INSERT.

- Compute `next_run_at` at ingest (O(1), scanner stays dumb).
- Reject invalid cron with 400.
- Sync insert (durable on ACK).
- Idempotency-Key header → `(tenant_id, key) → job_id` table with TTL.

### 5.2 Scheduler Fleet (sharded scanners)

One logical scheduler per `partition_id`. Leader election per shard (K8s Lease / etcd / PG advisory locks). Followers hot standby.

```sql
BEGIN;
  SELECT job_id, next_run_at, cron_expr, ...
  FROM job_schedule
  WHERE partition_id = $1 AND state = 'ACTIVE' AND next_run_at <= now()
  ORDER BY next_run_at
  LIMIT 1000
  FOR UPDATE SKIP LOCKED;

  INSERT INTO job_run (run_id, job_id, ..., scheduled_fire_at, next_run_at, status)
  VALUES (..., $next_run_at, $next_run_at, 'PENDING')
  ON CONFLICT (job_id, scheduled_fire_at) DO NOTHING;

  UPDATE job_schedule
  SET next_run_at = compute_next(cron_expr, next_run_at),
      last_run_at = next_run_at,
      version = version + 1
  WHERE job_id = $job_id AND version = $version;
COMMIT;

NOTIFY workers_partition_$1, 'work_available';
```

**Misfire policy:** If `next_run_at` is far in the past, default is fire once (coalesce). Configurable: `SKIP`, `FIRE_ONCE`, `FIRE_ALL`.

### 5.3 Worker Claim Protocol

```sql
UPDATE job_run
SET status = 'RUNNING',
    lease_owner = $worker_id,
    lease_expires_at = now() + $lease_ttl,
    started_at = now(),
    attempt = attempt + 1
WHERE run_id = (
  SELECT run_id FROM job_run
  WHERE partition_id = ANY($assigned_partitions)
    AND status = 'PENDING'
    AND next_run_at <= now()
  ORDER BY priority ASC, next_run_at ASC
  LIMIT 1
  FOR UPDATE SKIP LOCKED
)
RETURNING run_id, job_id, payload, timeout_secs, attempt, max_attempts;
```

**Lease semantics:**
- `lease_ttl = job.timeout_secs + 30s slack`
- Heartbeat every `lease_ttl / 3`: `UPDATE job_run SET lease_expires_at = now()+$ttl WHERE run_id=$id AND lease_owner=$worker_id`. If 0 rows affected, worker lost the lease → abort.
- Reaper reclaims `status='RUNNING' AND lease_expires_at < now()`:

```sql
UPDATE job_run
SET status = 'PENDING', lease_owner = NULL, next_run_at = now() + backoff(attempt)
WHERE status = 'RUNNING' AND lease_expires_at < now()
  AND attempt < max_attempts
RETURNING run_id;
```

Rows exceeding `max_attempts` → `status='DEAD'`.

**Idempotency:** Handlers idempotent on `run_id`. Optional effect ledger for side effects.

### 5.4 Notifications (Why No Queue in Critical Path)

| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| Polling only | Simplest | Latency; DB load | Fallback |
| PG LISTEN/NOTIFY | No new infra, transactional | 8KB payload, per-connection | Default |
| Redis pub/sub | Fast, simple | Not durable | Good at scale |
| SQS | Durable, DLQ | 15-min delay limit | Cross-region |
| Kafka | Replayable | Ops burden | Only if already present |

**Decision:** Table is source of truth. Notifications are hints. Polling is the safety net.

```
while running:
  claimed = try_claim()
  if claimed: execute(claimed); continue
  wait_for_notification(timeout=1s)
```

**Why not a queue in the critical path:** It becomes a second source of truth. You still need the table for "what's running," priority, quotas, cancellation. You'd reconcile two stores — a classic footgun.

### 5.5 Multi-Tenancy

- `partition_id = hash(tenant_id) % N` — tenant always lands on same shard.
- Per-tenant concurrency via Redis counter (`INCR` on claim, `DECR` on complete).
- Priority via `ORDER BY priority, next_run_at`.
- Per-tenant rate limits at ingestion.

### 5.6 Observability

- Metrics: `scheduler_scan_lag_seconds`, `job_run_claim_latency`, `job_run_duration`, `lease_expirations_total`, `retries_total`, `dead_letter_total`.
- Tracing: one trace per `run_id` from ingestion → scheduler → worker.
- Logs: JSON with `run_id`, `job_id`, `tenant_id`, `partition_id`, `attempt`.

### 5.7 Failure Modes

| Failure | Mitigation |
|---------|------------|
| Scheduler crash mid-scan | Transaction rollback; retry |
| Crash after insert, before schedule advance | Same transaction (atomic) |
| Worker crash mid-execution | Lease expiry → reaper → retry |
| Worker loses lease but keeps running | Heartbeat check before side effects + idempotency |
| DB primary failover | Multi-AZ, jittered retry |
| Notification down | Polling fallback |
| Clock skew | Use DB `now()` only |
| Thundering herd | Jittered claims, batch claims |
| Poison job | `max_attempts` → DEAD |
| DST edges | Timezone-aware cron lib; document policy |

### 5.8 Scaling

| Dimension | Strategy |
|-----------|----------|
| Ingestion | Horizontal pods |
| Scheduler | Shard by `partition_id` |
| Workers | Horizontal; partitioned subscription |
| DB | Read replicas; time-partition `job_run`; archive terminal rows |

---

## 6. Tradeoffs

| Decision | Alternative | Why |
|----------|-------------|-----|
| Table as source of truth | Queue as source | Queryability, priority, cancellation |
| Notification as hint | Queue in critical path | Avoid dual source of truth |
| Lease-based claim | Queue visibility timeout | Arbitrary `next_run_at`; queryable |
| `UNIQUE (job_id, scheduled_fire_at)` | App-level dedupe | DB-enforced, race-free |
| Partial indexes | Full indexes | Hot set in memory |
| Time-partitioned `job_run` | Single table | Bounded index; cheap archival |
| Per-partition leader | Single global | Linear scale; blast radius |
| At-least-once | Exactly-once | Impossible without cooperative handlers |
| Compute `next_run_at` at ingest | At scan | Scanner stays dumb |

---

## 7. Interviewer Deep-Dive Questions

**Q1: Two workers claim same job?**
Conditional `UPDATE` + `FOR UPDATE SKIP LOCKED`. One wins; loser sees 0 rows affected.

**Q2: Worker claims then pauses 10 min?**
Lease expires → reaper resets to PENDING → another worker claims. Original worker's heartbeat returns 0 rows → aborts before side effects.

**Q3: Slow-but-alive worker causes duplicate?**
Heartbeat keeps lease alive; handlers idempotent on `run_id`. At-least-once contract documented.

**Q4: Scheduler crashes after insert before advance?**
Same transaction → atomic. On restart, `UNIQUE` makes retry a no-op.

**Q5: Cron fires every second?**
Edge of scan cycle. Promote to dedicated high-frequency partition, or treat as stream processor with in-memory timers + durable checkpoint.

**Q6: 1M runs/sec?**
See Option C.

**Q7: 10B rows in `job_run`?**
Time-partition. Only current partition has PENDING rows. Partial index stays tiny.

**Q8: Thundering herd?**
Jittered notification delays, batch claims, priority scheduling, per-worker rate limits.

**Q9: Write amplification?**
Insert + claim + N heartbeats + complete ≈ 12 writes per 5-min job. At 10K runs/sec → 120K writes/sec. Mitigate: longer heartbeats, batch, or move lease to Redis.

**Q10: DST for cron?**
Timezone-aware lib. Policy: spring-forward skips, fall-back fires once.

**Q11: Non-idempotent handler?**
Effect ledger: handler calls `record_effect(run_id, key)` before side effect. Unique key rejects duplicates.

**Q12: Cancel running job?**
Set state=CANCELLED (no future runs) + status=CANCELLING for in-flight. Workers poll cancellation. Grace period, then revoke lease.

**Q13: Cron firing 1000x in past?**
Misfire policy: default FIRE_ONCE (coalesce). Never backfill 525K runs.

**Q14: Tenant ordering?**
All tenant jobs → same partition. Order by `priority, next_run_at`. Strict ordering = sticky worker (throughput cost).

**Q15: Deploy without losing jobs?**
Rolling deploy. Old workers drain (stop claiming, finish, exit). Leases on abandoned jobs expire → reclaimed.

**Q16: Notification channel down?**
Polling fallback after 1s timeout. Correctness unaffected.

**Q17: Debug wrong result?**
Logs + trace keyed on `run_id`. `result`/`error` in row. Archived to history/S3.

**Q18: Job exceeds timeout?**
Worker cancels handler at `timeout_secs`, marks FAILED. Lease TTL = timeout + slack.

**Q19: Why not K8s CronJobs?**
Per-cluster, 100s of jobs, no fairness, no queryable history, no cross-cluster.

**Q20: Why not Temporal?**
Great for DAGs/stateful workflows. Overkill for high-fan-out time scheduling.

**Q21: Biggest risk?**
The database. Mitigate: multi-AZ, replicas, sharding by partition, documented degradation.

**Q22: Evolve to exactly-once?**
Effectively-once: idempotent handlers + effect ledger + transactional outbox.
