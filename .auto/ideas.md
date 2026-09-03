# Realtime pipeline fan-out ideas

- Measure whether configuration lookup belongs at buffer construction instead of every `should_flush` call.
- If a timer is required, prove that task shutdown, empty-buffer behavior, write failures, retries, and restarts preserve the existing black-box contract.
- If sharding the buffer map helps, retain one logical buffer per existing pipeline, organization, destination, and batch-key tuple.
- If destination writes dominate after timer changes, profile `write_wal` before changing the shared ingest path.
