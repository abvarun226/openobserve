# Realtime pipeline fan-out ideas

## Current status
- Throughput: 49.4-50.9k records/s reproducibly, 100-120% above branch-v0.40.0
- Latency: 19-25 ms, 70-78% below branch-v0.40.0
- Both targets (100% improvement each) consistently met
- Remaining bottleneck: inherent VRL execution (14 calls/record, 28 value conversions/record) and Tokio channel scheduling

## Ideas for future work
- Strengthen the external benchmark oracle to reject duplicate and unexpected measured deliveries and validate measured local records; keep this outside optimization commits.
- Add a mixed and non-default `batch_key` black-box profile to cover grouping and per-key flush timing.

## Tried and discarded in segment 2
- Source node bypass (scheduling regression)
- VRL resolver Arc wrapping (within noise)
- Pipeline lookup optimization with has_executable_pipeline (within noise)
- Indexed node arrays replacing HashMaps (within noise)
- Source size sampling from first record (catastrophic regression — something downstream is sensitive to accurate source_size)
- Merged source_size estimation into feed loop (within noise)
- try_send for source feed (improved latency 17%, throughput within noise)

## Remaining ideas
- Fuse sequential linear function chains (common-01 → common-02) into a single task to eliminate one channel hop per record for the chained VRL transforms.
- Enterprise `get_pipeline_wal_writer` calls `update_writer_metrics` on EVERY lookup, scanning all registry entries. This is read-only enterprise code, so can't fix directly. Could cache the writer Arc across requests if the buffer_key is stable.
- Per-record VRL Value conversion (serde_json ↔ vrl::value) is likely the dominant cost. Each of 14 function nodes does 2 conversions per record = 28,000 conversions per 1000-record request. Can't change VRL library.
- Batching multiple records per channel message to reduce channel operation overhead (complex refactor).
- Reduce `send_to_children` for large fan-outs using concurrent sends (previous fan-out changes regressed).
