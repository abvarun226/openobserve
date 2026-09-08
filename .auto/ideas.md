# Realtime pipeline fan-out ideas

## Current status
- Throughput: 54.4k records/s, 126% above branch-v0.40.0
- Latency: 20.3 ms, 75% below branch-v0.40.0
- Both targets (100% improvement each) significantly exceeded
- VRL-native pipeline records eliminated 14 redundant serde↔vrl conversions per record

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
- Extended try_send to source+result channels (regressed 3.4%)
- Channel capacity 32 (within noise)
- Cooperative yield interval 256 (within noise)
- Pre-flatten skip for single-child functions (regressed 5% — flattened flag is needed)
- BATCH_BUFFER_SHARDS 64 (regressed 4.4%)

## Remaining ideas
- The VRL-native path currently skips pre-flatten. For multi-child fan-outs (common-02), re-add a VRL-native flatness check to set flattened=true and avoid 12 downstream flatten_with_level calls.
- Enterprise `get_pipeline_wal_writer` calls `update_writer_metrics` on EVERY lookup, scanning all registry entries. This is read-only enterprise code, so can't fix directly. Could cache the writer Arc across requests if the buffer_key is stable.
- Batching multiple records per channel message to reduce channel operation overhead (complex refactor).
- The leaf/RemoteStream into_owned conversion from VrlOwned to serde_json now happens at the boundary. Could batch these conversions or parallelize them.
- The source node currently sends serde_json Owned records. Could convert to VRL at the source and avoid the first function node's into_vrl conversion.
