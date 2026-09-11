Distributed Job Scheduler — Design Docs
This package contains three design documents, each representing a distinct architectural approach to building a distributed job scheduler at principal-engineer scale.

Doc	Approach	Best For
Option A	Table-as-source-of-truth, no queue	Teams optimizing for correctness, simplicity, and operational minimalism
Option B	Table + notification queue in hybrid mode	Teams needing push latency at scale with a durable broker already in place
Option C	Orchestrator + distributed locks + event log	Multi-region, multi-tenant, workflow-adjacent schedulers at 1M+ runs/sec
Quick Comparison
Dimension	Option A	Option B	Option C
Source of truth	DB table	DB table	Event log + DB projection
Claim mechanism	Lease + conditional UPDATE	Lease + conditional UPDATE	Distributed lock (Raft/etcd) + lease
Notification	Postgres NOTIFY / polling	Kafka / Redis Streams	Kafka + orchestrator push
Scheduling brain	Sharded scanners	Sharded scanners	Central orchestrator (Raft leader)
Latency (p99)	~1–2s	~100–200ms	~50–100ms
Operational complexity	Low	Medium	High
Max throughput	~10K runs/sec	~100K runs/sec	~1M+ runs/sec
Multi-region	Awkward	Good	Native
Failure blast radius	Per-shard	Per-shard	Leader election, quorum
Best at	1M runs/day	100M runs/day	1B+ runs/day
