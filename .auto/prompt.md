# Autoresearch: realtime pipeline fan-out

## Objective
Reduce `_bulk` request latency and increase completed remote-delivery throughput for a realtime pipeline with 12 RemoteStream branches. The workload matches the canary shape from `docs/canary-pipeline-bottleneck-2026-09-02.md`.

The candidate starts at OpenObserve commit `a78bdfd61b` from `autoresearch/bulk-ingest-throughput/01-bulk-ingest-13x`. The behavioral baseline is `branch-v0.40.0` at `cdf266a634`.

## Metrics
- **Primary:** `candidate_client_latency_ms` (milliseconds, lower is better). This is the candidate median client `_bulk` latency from five isolated latency trials.
- **Secondary:** `candidate_remote_delivery_latency_ms`, `candidate_prometheus_response_time_ms`, `candidate_throughput_records_per_sec`, `candidate_throughput_mb_per_sec`, `latency_vs_baseline_percent`, and `throughput_vs_baseline_percent`.

A latency improvement is not a win if it reduces throughput by more than 5%, loses deliveries, duplicates deliveries, changes transformed output, or breaks retry and restart recovery.

## How to run
Run `./.auto/measure.sh` from this worktree. The setup retains the matching enterprise `Cargo.toml` and `Cargo.lock` as local `assume-unchanged` build overlays. The script builds each binary without changing tracked source content, then runs the isolated baseline-versus-candidate benchmark.

The runner uses five fresh trials for each binary and profile. Each trial uses a new process, loopback receiver, SQLite metadata store, normal write-ahead log (WAL), remote-stream WAL, ports, credentials, and data root. It verifies every local record and every expected `(record identifier, branch marker)` RemoteStream delivery.

## Correctness gate
`./.auto/checks.sh` runs the existing black-box RemoteStream smoke scenarios against the current candidate binary:
- `healthy` verifies complete, exact local and remote delivery.
- `retry` verifies one HTTP 500 delivery attempt retries and produces exactly one accepted delivery.
- `restart` verifies acknowledged records survive a process restart when the receiver initially returns HTTP 503.

These checks validate public HTTP responses, externally observed receiver payloads, and public HTTP search results. They do not inspect queues, locks, buffers, or WAL internals.

## Files in scope
- `src/service/pipeline/batch_execution.rs` — RemoteStream buffering, flushing, branch execution, and destination concurrency.
- `src/service/logs/bulk.rs` — `_bulk` routing into realtime pipelines.
- `src/service/logs/ingest.rs` — pipeline input preparation and destination write routing.
- `src/service/logs/mod.rs` — destination writes, only if measurement identifies this path as material.
- `src/ingester/` — WAL or memtable code, only if measurement identifies a shared bottleneck.

Read-only supporting code:
- `/data/o2-autoresearch/realtime-pipeline-candidate/o2-enterprise/` — enterprise build manifest and RemoteStream implementation.
- `/data/o2-bench-bulk/scripts/run_remote_pipeline_benchmark.sh` — benchmark and black-box delivery oracle.
- `/data/o2-bench-bulk/scripts/run_remote_pipeline_smoke.sh` — black-box correctness gate.
- `/data/o2-bench-bulk/docs/canary-pipeline-bottleneck-2026-09-02.md` — production observations.

## Off limits
- Do not change the benchmark, smoke harness, fixtures, generated workload, or production configuration to improve a score.
- Do not change the public HTTP API, request acknowledgement rules, retry rules, restart recovery, record transformation, destination routing, usage reporting, or delivery cardinality.
- Do not change direct ingestion paths unless a change is required to preserve pipeline behavior.
- Do not modify `/data/o2-source/o2-enterprise/`.
- Do not add dependencies, unsafe code, background work that can lose records, or unbounded tasks.
- Do not commit the local enterprise `Cargo.toml` or `Cargo.lock` build overlays.

## Candidate ideas
1. Read `ZO_PIPELINE_BATCH_TIMEOUT_MS` instead of the 5,000 ms compile-time timeout in `BatchBuffer::should_flush`.
2. Replace request-triggered timeout checks with a bounded background flush mechanism that preserves WAL durability and retry behavior.
3. Remove global `BATCH_BUFFERS` lock contention without changing buffer-key isolation or flush ordering.
4. Reuse the existing bulk write fast path for nested-pipeline-free destination streams only when black-box output remains equivalent.

## What has been tried
- `send_to_children`: moved the final fan-out receiver out of the clone loop, eliminating one deep `PipelineItem` clone per fan-out. The candidate's latency median was 86.189 ms against the behavioral baseline's 85.345 ms. Discarded.
- `flush_all_buffers`: moved remote WAL writes outside the global `BATCH_BUFFERS` mutex. The candidate's latency median regressed from 81.541 ms to 82.410 ms, with remote delivery latency also worse. Discarded.
- The earlier bulk-ingest optimizations improve the direct path but bypass realtime pipelines.
- The canary has idle CPU, disk, and WAL locks, while `_bulk` mean latency and pipeline execution time are both about five seconds.
- The current RemoteStream buffer uses 50 records, 32 KiB, or a hardcoded five-second timeout. At roughly five records per second per destination, the timer dominates.
- The current global `BATCH_BUFFERS` mutex serializes branches and requests before any remote WAL write.
- The current candidate baseline measures 84.395 ms client latency, 1,609.008 ms remote-delivery latency, and 26,006.2 records/s throughput. It is 8.37% slower in client latency and 3.01% faster in throughput than `branch-v0.40.0`; use this only as a starting point because the runner takes fresh measurements every iteration.
