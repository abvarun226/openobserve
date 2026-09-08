# Realtime pipeline fan-out ideas

## Current status
- Throughput: 55-57k records/s reproducibly, 119-131% above branch-v0.40.0
- Latency: 18-24 ms, 72-78% below branch-v0.40.0
- Both targets (100% improvement each) significantly exceeded

## Key retained optimizations (segment 2)
1. Non-allocating estimate_json_bytes (replaced to_string/ByteCounter)
2. Lock-free flush_all_buffers (shard locks dropped before async WAL writes)
3. Pipeline inputs capacity fix (actual record count, not destination count)
4. try_send in send_to_children (avoids 13k unnecessary yields per 1000-record request)
5. VRL-native pipeline records (function nodes carry vrl::value::Value, skipping 14 serde↔vrl conversions/record)
6. VRL-native flatness check (prevents 12 downstream flatten_with_level calls/record)

## Tried and discarded in segment 2
- Source node bypass, VRL resolver Arc wrapping, indexed node arrays, pipeline lookup optimization, merged source_size estimation — all within noise
- Source size sampling from first record — catastrophic regression (source_size must be accurate)
- try_send extensions to source/result channels — scheduling regression
- Channel capacity tuning (32 or 128) — within noise
- Cooperative yield interval — within noise
- Pre-flatten skip for single-child functions — regressed 5% (flattened flag is needed)
- BATCH_BUFFER_SHARDS 64 — regressed 4.4%
- Source feed VRL conversion — regressed 2.5% (serializes pipelined work)
- Direct VRL clone fan-out — regressed 5.6% (front-loads memory allocation vs deferred Arc)

## Remaining ideas
- Enterprise `get_pipeline_wal_writer` calls `update_writer_metrics` on EVERY lookup. Read-only code, can't fix. Could cache the writer Arc if buffer_key is stable.
- The RemoteStream into_owned VRL→serde conversion is now the biggest per-record cost (12 × 1000/request). Could batch or parallelize these conversions.
- Batching multiple records per channel message to reduce channel overhead (complex refactor).
- The leaf node's into_owned does VRL→serde; if the leaf stream matches the source, the conversion result is the same as the pipeline input. Could pass the original serde_json Value alongside the VRL record to avoid reconversion at the leaf.
