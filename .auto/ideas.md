# Realtime pipeline fan-out ideas

- Strengthen the external benchmark oracle to reject duplicate and unexpected measured deliveries and validate measured local records; keep this outside optimization commits.
- Add a mixed and non-default `batch_key` black-box profile to cover grouping and per-key flush timing.

- Measure whether configuration lookup belongs at buffer construction instead of every `should_flush` call.
- If a timer is required, prove that task shutdown, empty-buffer behavior, write failures, retries, and restarts preserve the existing black-box contract.
- If sharding the buffer map helps, retain one logical buffer per existing pipeline, organization, destination, and batch-key tuple.
- Profile the enterprise `get_pipeline_wal_writer` path before changing it. It takes a shared `RwLock`, scans writer metrics, and selects a random writer for every flushed RemoteStream batch.
- If destination writes dominate after timer changes, profile `write_wal` before changing the shared ingest path.
