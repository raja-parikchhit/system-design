# Distributed Job Scheduler — Design Docs

Three architectural approaches to a distributed job scheduler at principal-engineer scale.

| Doc | Approach | Best For |
|-----|----------|----------|
| Option A | Table-as-source-of-truth, no queue | Correctness, simplicity, operational minimalism |
| Option B | Table + notification queue (hybrid) | Push latency at scale with durable broker |
| Option C | Orchestrator + distributed locks + event log | Multi-region, workflow-adjacent, 1M+ runs/sec |

## Quick Comparison

| Dimension | Option A | Option B | Option C |
|-----------|----------|----------|----------|
| Source of truth | DB table | DB table | Event log + DB projection |
| Claim mechanism | Lease + conditional UPDATE | Lease + conditional UPDATE | Distributed lock (Raft/etcd) + lease |
| Notification | Postgres NOTIFY / polling | Kafka / Redis Streams | Kafka + orchestrator push |
| Scheduling brain | Sharded scanners | Sharded scanners | Central orchestrator (Raft leader) |
| Latency (p99) | ~1–2s | ~100–200ms | ~50–100ms |
| Operational complexity | Low | Medium | High |
| Max throughput | ~10K runs/sec | ~100K runs/sec | ~1M+ runs/sec |
| Multi-region | Awkward | Good | Native |
| Failure blast radius | Per-shard | Per-shard | Leader election, quorum |
| Best at | 1M runs/day | 100M runs/day | 1B+ runs/day |

## How to Use in an Interview

1. Start with Option A. It's the correct default. Explain why a queue is *not* in the critical path.
2. Pivot to Option B when pushed on latency or scale. Show you know when a queue helps vs. hurts.
3. Reach for Option C only for multi-region / exactly-once / 1M+ runs/sec. Call out the over-engineering risk.

## Files
- `option-a-no-queue.md`
- `option-b-with-queue.md`
- `option-c-orchestrator-locks.md`
