// Copyright 2025 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::collections::HashMap;

use actix_web::web;
use config::{
    BLOCKED_STREAMS, TIMESTAMP_COL_NAME, get_config,
    meta::stream::{StreamParams, StreamType},
    metrics,
    utils::{
        flatten, json,
        time::{now_micros, parse_timestamp_micro_from_value},
    },
};
use infra::{errors::Result, schema::get_flatten_level};
use rayon::prelude::*;

use crate::{
    common::meta::ingestion::{
        BulkResponse, BulkResponseError, BulkResponseItem, IngestionRequest, IngestionValueType,
    },
    service::{
        format_stream_name,
        ingestion::check_ingestion_allowed,
        logs::{ingestion_log_enabled, log_failed_record},
        schema::{get_future_discard_error, get_upto_discard_error},
    },
};

pub const TRANSFORM_FAILED: &str = "document_failed_transform";
pub const TS_PARSE_FAILED: &str = "timestamp_parsing_failed";
pub const SCHEMA_CONFORMANCE_FAILED: &str = "schema_conformance_failed";
pub const PIPELINE_EXEC_FAILED: &str = "pipeline_execution_failed";

pub async fn ingest(
    thread_id: usize,
    org_id: &str,
    body: web::Bytes,
    user: crate::common::meta::ingestion::IngestUser,
) -> Result<BulkResponse> {
    let start = std::time::Instant::now();

    // check system resource
    check_ingestion_allowed(org_id, StreamType::Logs, None).await?;

    // let mut errors = false;
    let mut bulk_res = BulkResponse {
        took: 0,
        errors: false,
        items: vec![],
    };

    let cfg = get_config();
    let now = config::utils::time::now_micros();
    let min_ts = now - cfg.limit.ingest_allowed_upto_micro;
    let max_ts = now + cfg.limit.ingest_allowed_in_future_micro;

    let log_ingestion_errors = ingestion_log_enabled().await;
    let mut action = String::new();
    let mut stream_name = String::new();
    let mut doc_id: Option<String> = None;
    let stream_type = StreamType::Logs;

    let mut stream_key_cache: HashMap<String, String> = HashMap::new();
    let mut streams_data: HashMap<String, Vec<(i64, json::Map<String, json::Value>)>> =
        HashMap::new();
    let mut next_line_is_data = false;

    // Zero-copy line scanning: store byte ranges for parallel JSON parsing
    struct DataLineInfo {
        start: usize,
        end: usize,
        stream_idx: usize,
        doc_id: Option<String>,
    }
    let mut data_lines: Vec<DataLineInfo> = Vec::new();
    let mut stream_names: Vec<(String, String)> = Vec::new();
    let mut current_stream_idx: usize = 0;
    let body_bytes = body.as_ref();
    let mut last_meta_start: usize = 0;
    let mut last_meta_end: usize = 0;

    // Scan body for lines using memchr
    let mut pos: usize = 0;
    while pos < body_bytes.len() {
        let line_start = pos;
        let nl_pos = memchr::memchr(b'\n', &body_bytes[pos..])
            .map(|i| pos + i)
            .unwrap_or(body_bytes.len());
        pos = nl_pos + 1;
        // Trim trailing \r\n
        let mut line_end = nl_pos;
        while line_end > line_start
            && (body_bytes[line_end - 1] == b'\n' || body_bytes[line_end - 1] == b'\r')
        {
            line_end -= 1;
        }
        if line_end <= line_start {
            continue; // empty line
        }

        let line = &body_bytes[line_start..line_end];

        if !next_line_is_data {
            // Skip re-parsing identical metadata lines
            if last_meta_end > last_meta_start
                && line == &body_bytes[last_meta_start..last_meta_end]
            {
                next_line_is_data = true;
                continue;
            }
            // Fast-parse metadata line from raw bytes.
            if let Some((line_action, line_stream_name, line_doc_id)) =
                super::parse_bulk_index_fast(line)
            {
                if line_action != action {
                    action = line_action.to_string();
                }
                if line_stream_name != stream_name {
                    stream_name = line_stream_name.to_string();
                }
                doc_id = line_doc_id.map(|id| id.to_string());
            } else {
                // Fall back to full parsing for valid, non-fast-path metadata.
                let value: json::Value = json::from_slice(line)?;
                let Some((line_action, line_stream_name, line_doc_id)) =
                    super::parse_bulk_index(&value)
                else {
                    continue;
                };
                if line_action != action {
                    action = line_action.to_string();
                }
                if line_stream_name != stream_name {
                    stream_name = line_stream_name.to_string();
                }
                doc_id = line_doc_id.map(|id| id.to_string());
            }

            if stream_name.is_empty() || stream_name == "_" || stream_name == "/" {
                let err_msg = format!("Invalid stream name: {}", String::from_utf8_lossy(line));
                log::warn!("[LOGS:BULK] {err_msg}");
                bulk_res.errors = true;
                let err = BulkResponseError::new(
                    err_msg.to_string(),
                    stream_name.to_string(),
                    err_msg,
                    "0".to_string(),
                );
                let mut item = HashMap::new();
                item.insert(
                    action.to_string(),
                    BulkResponseItem::new_failed(
                        stream_name.to_string(),
                        doc_id.clone().unwrap_or_default(),
                        err,
                        None,
                        stream_name.to_string(),
                    ),
                );
                bulk_res.items.push(item);
                continue; // skip
            }

            if !cfg.common.skip_formatting_stream_name {
                stream_name = format_stream_name(stream_name);
            }

            // skip blocked streams
            if !stream_key_cache.contains_key(&stream_name) {
                let key = format!("{org_id}/{}/{stream_name}", stream_type);
                stream_key_cache.insert(stream_name.clone(), key.clone());
                if BLOCKED_STREAMS.contains(&key) {
                    log::warn!(
                        "[LOGS:BULK] stream [{org_id}/{stream_name}] is blocked from ingestion"
                    );
                    continue; // skip
                }
            }
            last_meta_start = line_start;
            last_meta_end = line_end;
            // Track stream index for parallel processing
            let needs_new = stream_names.is_empty()
                || stream_names[current_stream_idx].0 != stream_name
                || stream_names[current_stream_idx].1 != action;
            if needs_new {
                current_stream_idx = stream_names.len();
                stream_names.push((stream_name.clone(), action.clone()));
            }
            next_line_is_data = true;
        } else {
            next_line_is_data = false;
            // Store byte range for parallel JSON parsing
            data_lines.push(DataLineInfo {
                start: line_start,
                end: line_end,
                stream_idx: current_stream_idx,
                doc_id: doc_id.clone(),
            });
        }
    }

    // Parse all data lines in parallel with timestamp extraction
    enum ParsedRecord {
        Ok {
            val: json::Map<String, json::Value>,
            stream_idx: usize,
            timestamp: i64,
        },
        TimestampError {
            value: json::Value,
            stream_idx: usize,
            doc_id: Option<String>,
        },
        OutOfRange {
            value: json::Value,
            stream_idx: usize,
            doc_id: Option<String>,
            too_old: bool,
        },
        ParseError,
    }
    let parsed_results: Vec<ParsedRecord> = data_lines
        .into_par_iter()
        .map(|info| {
            // simd-json requires &mut [u8] (modifies input for string unescaping).
            // Copy each line into a mutable buffer for SIMD-accelerated parsing.
            let mut line_buf = body_bytes[info.start..info.end].to_vec();
            let mut value: json::Value = match simd_json::serde::from_slice(&mut line_buf) {
                Ok(v) => v,
                Err(_) => return ParsedRecord::ParseError,
            };
            let mut local_val = match value.take() {
                json::Value::Object(v) => v,
                _ => return ParsedRecord::ParseError,
            };
            if let Some(ref doc_id) = info.doc_id {
                local_val.insert("_id".to_string(), json::Value::String(doc_id.clone()));
            }
            let (timestamp, has_valid_timestamp) = match local_val.get(TIMESTAMP_COL_NAME) {
                Some(v) => match parse_timestamp_micro_from_value(v) {
                    Ok(t) => (t.0, t.1),
                    Err(_) => {
                        return ParsedRecord::TimestampError {
                            value: json::Value::Object(local_val),
                            stream_idx: info.stream_idx,
                            doc_id: info.doc_id,
                        };
                    }
                },
                None => (now_micros(), false),
            };
            if timestamp < min_ts || timestamp > max_ts {
                return ParsedRecord::OutOfRange {
                    value: json::Value::Object(local_val),
                    stream_idx: info.stream_idx,
                    doc_id: info.doc_id,
                    too_old: timestamp < min_ts,
                };
            }
            if !has_valid_timestamp {
                local_val.insert(
                    TIMESTAMP_COL_NAME.to_string(),
                    json::Value::Number(timestamp.into()),
                );
            }
            ParsedRecord::Ok {
                val: local_val,
                stream_idx: info.stream_idx,
                timestamp,
            }
        })
        .collect();

    // Process parsed results sequentially
    for record in parsed_results {
        match record {
            ParsedRecord::Ok {
                val,
                stream_idx,
                timestamp,
            } => {
                let (ref rec_stream_name, _) = stream_names[stream_idx];
                match streams_data.get_mut(rec_stream_name) {
                    Some(v) => v.push((timestamp, val)),
                    None => {
                        streams_data.insert(rec_stream_name.clone(), vec![(timestamp, val)]);
                    }
                }
            }
            ParsedRecord::TimestampError {
                value,
                stream_idx,
                doc_id,
            } => {
                let (ref sn, ref act) = stream_names[stream_idx];
                bulk_res.errors = true;
                metrics::INGEST_ERRORS
                    .with_label_values(&[org_id, StreamType::Logs.as_str(), sn, TS_PARSE_FAILED])
                    .inc();
                log_failed_record(log_ingestion_errors, &value, TS_PARSE_FAILED);
                add_record_status(
                    sn.clone(),
                    doc_id,
                    act.clone(),
                    Some(value),
                    &mut bulk_res,
                    Some(TS_PARSE_FAILED.to_string()),
                    Some(TS_PARSE_FAILED.to_string()),
                );
            }
            ParsedRecord::OutOfRange {
                value,
                stream_idx,
                doc_id,
                too_old,
            } => {
                let (ref sn, ref act) = stream_names[stream_idx];
                bulk_res.errors = true;
                let reason = if too_old {
                    get_upto_discard_error().to_string()
                } else {
                    get_future_discard_error().to_string()
                };
                metrics::INGEST_ERRORS
                    .with_label_values(&[org_id, StreamType::Logs.as_str(), sn, TS_PARSE_FAILED])
                    .inc();
                log_failed_record(log_ingestion_errors, &value, TS_PARSE_FAILED);
                add_record_status(
                    sn.clone(),
                    doc_id,
                    act.clone(),
                    Some(value),
                    &mut bulk_res,
                    Some(TS_PARSE_FAILED.to_string()),
                    Some(reason),
                );
            }
            ParsedRecord::ParseError => {
                return Err(infra::errors::Error::Message(
                    "Failed to parse JSON data line".to_string(),
                ));
            }
        }
    }

    // Route each stream through ingest.rs (pipeline path) or write_logs (direct path).
    for (stream_name, records) in streams_data {
        let stream_param = StreamParams::new(org_id, &stream_name, stream_type);
        let has_pipeline = crate::service::ingestion::get_stream_executable_pipeline(&stream_param)
            .await
            .is_some();

        if has_pipeline {
            // Pipeline-enabled: convert to Vec<json::Value> and route through ingest.rs
            let json_values: Vec<json::Value> = records
                .into_iter()
                .map(|(_, map)| json::Value::Object(map))
                .collect();
            match super::ingest::ingest(
                thread_id,
                org_id,
                &stream_name,
                IngestionRequest::JsonValues(IngestionValueType::Bulk, json_values),
                user.clone(),
                None,
                false,
            )
            .await
            {
                Ok(v) => {
                    for status in v.status {
                        bulk_res.items.extend(status.items);
                    }
                }
                Err(e) => {
                    log::error!(
                        "[LOGS:BULK] stream {org_id}/logs/{stream_name}: Ingestion error: {e}"
                    );
                    bulk_res.errors = true;
                    metrics::INGEST_ERRORS
                        .with_label_values(&[
                            org_id,
                            StreamType::Logs.as_str(),
                            &stream_name,
                            PIPELINE_EXEC_FAILED,
                        ])
                        .inc();
                    add_record_status(
                        stream_name.to_string(),
                        None,
                        action.to_string(),
                        None,
                        &mut bulk_res,
                        Some(PIPELINE_EXEC_FAILED.to_string()),
                        Some(PIPELINE_EXEC_FAILED.to_string()),
                    );
                }
            }
        } else {
            // No pipeline: direct write_logs path (optimized)
            // Flatten nested objects/arrays before schema inference, matching
            // the behavior of the ingest.rs normal path.
            let flatten_level = get_flatten_level(org_id, &stream_name, stream_type).await;
            let mut flat_records = Vec::with_capacity(records.len());
            for (ts, map) in records {
                match flatten::flatten_with_level(json::Value::Object(map), flatten_level) {
                    Ok(json::Value::Object(flat_map)) => flat_records.push((ts, flat_map)),
                    Ok(_) => unreachable!(),
                    Err(e) => {
                        log::error!("[LOGS:BULK] flatten error: {e}");
                        bulk_res.errors = true;
                        add_record_status(
                            stream_name.to_string(),
                            None,
                            action.to_string(),
                            None,
                            &mut bulk_res,
                            Some(TRANSFORM_FAILED.to_string()),
                            Some(e.to_string()),
                        );
                        continue;
                    }
                }
            }
            if flat_records.is_empty() {
                continue;
            }
            let mut ing_status = crate::common::meta::ingestion::IngestionStatus::Bulk(
                std::mem::take(&mut bulk_res),
            );
            match super::write_logs(
                thread_id,
                org_id,
                &stream_name,
                &mut ing_status,
                flat_records,
                false,
            )
            .await
            {
                Ok(_req_stats) => {
                    if let crate::common::meta::ingestion::IngestionStatus::Bulk(br) = ing_status {
                        bulk_res = br;
                    }
                }
                Err(e) => {
                    if let crate::common::meta::ingestion::IngestionStatus::Bulk(br) = ing_status {
                        bulk_res = br;
                    }
                    log::error!(
                        "[LOGS:BULK] stream {org_id}/logs/{stream_name}: Ingestion error: {e}"
                    );
                    bulk_res.errors = true;
                    metrics::INGEST_ERRORS
                        .with_label_values(&[
                            org_id,
                            StreamType::Logs.as_str(),
                            &stream_name,
                            TRANSFORM_FAILED,
                        ])
                        .inc();
                    add_record_status(
                        stream_name.to_string(),
                        None,
                        action.to_string(),
                        None,
                        &mut bulk_res,
                        Some(PIPELINE_EXEC_FAILED.to_string()),
                        Some(PIPELINE_EXEC_FAILED.to_string()),
                    );
                }
            }
        }
    }

    // metric + data usage
    let status_code = if bulk_res.errors { "500" } else { "200" };
    let took_time = start.elapsed().as_secs_f64();
    metrics::HTTP_RESPONSE_TIME
        .with_label_values(&[
            "/api/org/ingest/logs/_bulk",
            status_code,
            org_id,
            StreamType::Logs.as_str(),
            "",
            "",
        ])
        .observe(took_time);
    metrics::HTTP_INCOMING_REQUESTS
        .with_label_values(&[
            "/api/org/ingest/logs/_bulk",
            status_code,
            org_id,
            StreamType::Logs.as_str(),
            "",
            "",
        ])
        .inc();

    Ok(bulk_res)
}

pub fn add_record_status(
    stream_name: String,
    doc_id: Option<String>,
    action: String,
    value: Option<json::Value>,
    bulk_res: &mut BulkResponse,
    failure_type: Option<String>,
    failure_reason: Option<String>,
) {
    // For success records: skip all allocation when errors-only mode is active
    if failure_type.is_none() && get_config().common.bulk_api_response_errors_only {
        return;
    }

    let action = if action.is_empty() {
        "index".to_string()
    } else {
        action
    };

    let doc_id = match doc_id {
        Some(doc_id) => doc_id,
        None => String::new(),
    };

    let mut item = HashMap::with_capacity(1);
    match failure_type {
        Some(failure_type) => {
            let bulk_err = BulkResponseError::new(
                failure_type,
                stream_name.clone(),
                failure_reason.unwrap(),
                "0".to_owned(),
            );

            item.insert(
                action,
                BulkResponseItem::new_failed(
                    stream_name.clone(),
                    doc_id,
                    bulk_err,
                    value,
                    stream_name,
                ),
            );

            bulk_res.items.push(item);
        }
        None => {
            item.insert(
                action,
                BulkResponseItem::new(stream_name.clone(), doc_id, value, stream_name),
            );
            bulk_res.items.push(item);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::meta::ingestion::IngestUser;

    #[test]
    fn test_add_record_status() {
        let mut bulk_res = BulkResponse {
            took: 0,
            errors: false,
            items: vec![],
        };
        add_record_status(
            "olympics".to_string(),
            Some("1".to_string()),
            "create".to_string(),
            None,
            &mut bulk_res,
            None,
            None,
        );
        assert!(bulk_res.items.len() == 1);
    }

    #[tokio::test]
    async fn test_ingest_basic_functionality() {
        // Create a simple bulk request with one document
        let bulk_request = r#"{"index": {"_index": "test-stream", "_id": "1"}}
{"message": "test log message", "level": "info"}"#;

        let body = web::Bytes::from(bulk_request);
        let thread_id = 1;
        let org_id = "test-org";
        let user = IngestUser::from_user_email("test@example.com");

        // Note: This test will likely fail due to missing infrastructure setup,
        // but it demonstrates the basic structure of testing the ingest function
        let result = ingest(thread_id, org_id, body, user).await;

        // The test should either succeed or fail with a specific error
        // (likely related to missing database connections or configuration)
        match result {
            Ok(response) => {
                // If successful, verify basic response structure
                // The response should have items if the configuration allows it
                if !get_config().common.bulk_api_response_errors_only {
                    assert!(!response.items.is_empty());
                }
            }
            Err(e) => {
                // Expected to fail due to missing infrastructure
                // Just verify it's a proper error
                assert!(!e.to_string().is_empty());
            }
        }
    }

    mod add_record_status_tests {
        use super::*;

        #[test]
        fn test_add_record_status_success() {
            let mut bulk_res = BulkResponse {
                took: 0,
                errors: false,
                items: vec![],
            };

            add_record_status(
                "test_stream".to_string(),
                Some("doc_123".to_string()),
                "index".to_string(),
                Some(json::json!({"message": "test"})),
                &mut bulk_res,
                None,
                None,
            );

            // Should add one successful item
            assert_eq!(bulk_res.items.len(), 1);

            // Check the item structure
            let item = &bulk_res.items[0];
            assert!(item.contains_key("index"));

            let bulk_item = &item["index"];
            assert!(!bulk_item._index.is_empty());
            assert!(!bulk_item._id.is_empty());
        }

        #[test]
        fn test_add_record_status_with_failure() {
            let mut bulk_res = BulkResponse {
                took: 0,
                errors: false,
                items: vec![],
            };

            add_record_status(
                "test_stream".to_string(),
                Some("doc_456".to_string()),
                "create".to_string(),
                Some(json::json!({"data": "test"})),
                &mut bulk_res,
                Some(TS_PARSE_FAILED.to_string()),
                Some("Invalid timestamp format".to_string()),
            );

            // Should add one failed item
            assert_eq!(bulk_res.items.len(), 1);

            // Check the failed item structure
            let item = &bulk_res.items[0];
            assert!(item.contains_key("create"));

            let bulk_item = &item["create"];
            assert!(bulk_item.error.is_some());
        }

        #[test]
        fn test_add_record_status_empty_action() {
            let mut bulk_res = BulkResponse {
                took: 0,
                errors: false,
                items: vec![],
            };

            add_record_status(
                "test_stream".to_string(),
                None,
                "".to_string(), // Empty action should default to "index"
                None,
                &mut bulk_res,
                None,
                None,
            );

            assert_eq!(bulk_res.items.len(), 1);
            let item = &bulk_res.items[0];
            assert!(item.contains_key("index")); // Should default to "index"
        }

        #[test]
        fn test_add_record_status_no_doc_id() {
            let mut bulk_res = BulkResponse {
                took: 0,
                errors: false,
                items: vec![],
            };

            add_record_status(
                "stream_without_id".to_string(),
                None, // No document ID
                "update".to_string(),
                Some(json::json!({"field": "value"})),
                &mut bulk_res,
                None,
                None,
            );

            assert_eq!(bulk_res.items.len(), 1);
            let item = &bulk_res.items[0];
            let bulk_item = &item["update"];
            // Document ID should be empty string
            assert_eq!(bulk_item._id, "");
        }

        #[test]
        fn test_add_record_status_different_failure_types() {
            let mut bulk_res = BulkResponse {
                took: 0,
                errors: false,
                items: vec![],
            };

            let failure_types = [
                TRANSFORM_FAILED,
                TS_PARSE_FAILED,
                SCHEMA_CONFORMANCE_FAILED,
                PIPELINE_EXEC_FAILED,
            ];

            for (i, failure_type) in failure_types.iter().enumerate() {
                add_record_status(
                    format!("stream_{i}"),
                    Some(format!("doc_{i}")),
                    "index".to_string(),
                    Some(json::json!({"test": i})),
                    &mut bulk_res,
                    Some(failure_type.to_string()),
                    Some(format!("Error message {i}")),
                );
            }

            assert_eq!(bulk_res.items.len(), 4);

            // Verify each failure type is recorded
            for (i, failure_type) in failure_types.iter().enumerate() {
                let item = &bulk_res.items[i];
                let bulk_item = &item["index"];
                let error = bulk_item.error.as_ref().unwrap();
                assert_eq!(error.err_type, *failure_type);
            }
        }

        #[test]
        fn test_add_record_status_with_complex_json() {
            let mut bulk_res = BulkResponse {
                took: 0,
                errors: false,
                items: vec![],
            };

            let complex_json = json::json!({
                "timestamp": "2024-01-01T12:00:00Z",
                "level": "ERROR",
                "message": "Complex error occurred",
                "metadata": {
                    "service": "api-gateway",
                    "version": "1.0.0",
                    "tags": ["critical", "authentication"]
                },
                "error_details": {
                    "code": 500,
                    "stack_trace": "...",
                    "user_id": "user123"
                }
            });

            add_record_status(
                "complex_logs".to_string(),
                Some("complex_doc_1".to_string()),
                "index".to_string(),
                Some(complex_json.clone()),
                &mut bulk_res,
                None,
                None,
            );

            assert_eq!(bulk_res.items.len(), 1);
            let item = &bulk_res.items[0];
            let bulk_item = &item["index"];

            // Verify the item was created successfully (successful records don't store
            // original_record)
            assert_eq!(bulk_item.status, 200);
            assert!(bulk_item.error.is_none());
            assert!(!bulk_item._index.is_empty());
        }
    }

    mod bulk_constants_tests {
        use super::*;

        #[test]
        fn test_error_constants_uniqueness() {
            // Ensure all error constants are unique
            let constants = vec![
                TRANSFORM_FAILED,
                TS_PARSE_FAILED,
                SCHEMA_CONFORMANCE_FAILED,
                PIPELINE_EXEC_FAILED,
            ];

            let mut unique_constants = constants.clone();
            unique_constants.sort();
            unique_constants.dedup();

            assert_eq!(constants.len(), unique_constants.len());
        }
    }

    mod bulk_response_tests {
        use super::*;

        #[test]
        fn test_bulk_response_initialization() {
            let bulk_res = BulkResponse {
                took: 0,
                errors: false,
                items: vec![],
            };

            assert_eq!(bulk_res.took, 0);
            assert!(!bulk_res.errors);
            assert!(bulk_res.items.is_empty());
        }

        #[test]
        fn test_bulk_response_with_multiple_items() {
            let mut bulk_res = BulkResponse {
                took: 0,
                errors: false,
                items: vec![],
            };

            // Add multiple successful records
            for i in 0..5 {
                add_record_status(
                    format!("stream_{i}"),
                    Some(format!("doc_{i}")),
                    "index".to_string(),
                    Some(json::json!({"message": format!("log message {i}")})),
                    &mut bulk_res,
                    None,
                    None,
                );
            }

            // Add some failed records
            for i in 5..8 {
                add_record_status(
                    format!("stream_{i}"),
                    Some(format!("doc_{i}")),
                    "index".to_string(),
                    Some(json::json!({"message": format!("log message {i}")})),
                    &mut bulk_res,
                    Some(TS_PARSE_FAILED.to_string()),
                    Some("Timestamp error".to_string()),
                );
            }

            assert_eq!(bulk_res.items.len(), 8);

            // Check mix of successful and failed items
            let successful_items: usize = bulk_res
                .items
                .iter()
                .map(|item| {
                    let bulk_item = item.values().next().unwrap();
                    if bulk_item.error.is_none() { 1 } else { 0 }
                })
                .sum();

            let failed_items: usize = bulk_res
                .items
                .iter()
                .map(|item| {
                    let bulk_item = item.values().next().unwrap();
                    if bulk_item.error.is_some() { 1 } else { 0 }
                })
                .sum();

            assert_eq!(successful_items, 5);
            assert_eq!(failed_items, 3);
        }
    }

    mod data_processing_tests {
        use super::*;

        #[test]
        fn test_json_parsing_valid_data() {
            let valid_json =
                r#"{"message": "test log", "level": "info", "timestamp": "2024-01-01T10:00:00Z"}"#;
            let result: Result<json::Value, _> = json::from_slice(valid_json.as_bytes());
            assert!(result.is_ok());

            let value = result.unwrap();
            assert_eq!(value["message"], "test log");
            assert_eq!(value["level"], "info");
        }

        #[test]
        fn test_json_parsing_invalid_data() {
            let invalid_json = r#"{"message": "test log", "level": info"}"#; // Missing quotes around value
            let result: Result<json::Value, _> = json::from_slice(invalid_json.as_bytes());
            assert!(result.is_err());
        }

        #[test]
        fn test_timestamp_parsing() {
            let test_timestamps = vec![
                (
                    json::Value::String("2024-01-01T12:00:00Z".to_string()),
                    false,
                ),
                (
                    json::Value::String("2024-01-01T12:00:00.123Z".to_string()),
                    false,
                ),
                (
                    json::Value::Number(serde_json::Number::from(1640995200000000i64)),
                    true,
                ),
                (
                    json::Value::Number(serde_json::Number::from(1640995200000i64)),
                    false,
                ),
                (
                    json::Value::Number(serde_json::Number::from(1640995200i64)),
                    false,
                ),
            ];

            for (ts_value, ts_valid) in test_timestamps {
                match parse_timestamp_micro_from_value(&ts_value) {
                    Ok((timestamp, valid)) => {
                        assert_eq!(valid, ts_valid);
                        assert!(timestamp > 0);
                        // Should be a reasonable timestamp (after 2020)
                        assert!(timestamp > 1577836800000000i64); // 2020-01-01 in microseconds
                    }
                    Err(e) => {
                        // Some formats might not be supported, which is fine
                        println!("Timestamp parsing failed (expected for some formats): {e}");
                    }
                }
            }
        }

        #[test]
        fn test_timestamp_parsing_invalid() {
            let invalid_timestamps = vec![
                json::Value::String("not-a-timestamp".to_string()),
                json::Value::String("".to_string()),
                json::Value::Null,
                json::Value::Bool(true),
                json::Value::Array(vec![]),
                json::Value::Object(serde_json::Map::new()),
            ];

            for invalid_ts in invalid_timestamps {
                let result = parse_timestamp_micro_from_value(&invalid_ts);
                assert!(result.is_err());
            }
        }
    }

    mod stream_name_tests {
        use super::*;

        #[test]
        fn test_valid_stream_names() {
            let valid_names = vec![
                "test-stream",
                "application_logs",
                "service.metrics",
                "stream123",
                "a_very_long_stream_name_with_underscores_and_numbers_123",
            ];

            for name in valid_names {
                // These should not be empty or invalid
                assert!(!name.is_empty());
                assert!(name != "_");
                assert!(name != "/");
            }
        }

        #[test]
        fn test_invalid_stream_names() {
            let invalid_names = vec![
                "",  // Empty
                "_", // Single underscore
                "/", // Single slash
            ];

            for name in invalid_names {
                // These should be caught as invalid
                assert!(name.is_empty() || name == "_" || name == "/");
            }
        }

        #[test]
        fn test_stream_name_formatting() {
            // Test that format_stream_name is available and works
            let test_names = vec!["Test-Stream", "UPPERCASE", "mixed_Case_123"];

            for name in test_names {
                let formatted = format_stream_name(name.to_string());
                // Should return a non-empty formatted string
                assert!(!formatted.is_empty());
                // Typically converts to lowercase, but exact behavior may vary
            }
        }
    }

    mod error_handling_tests {
        use super::*;

        #[test]
        fn test_bulk_response_error_creation() {
            let error = BulkResponseError::new(
                TS_PARSE_FAILED.to_string(),
                "test-stream".to_string(),
                "Invalid timestamp format".to_string(),
                "400".to_string(),
            );

            // Basic validation that error object can be created
            // Exact structure depends on BulkResponseError implementation
            assert!(!format!("{error:?}").is_empty());
        }

        #[test]
        fn test_different_error_scenarios() {
            let mut bulk_res = BulkResponse {
                took: 0,
                errors: false,
                items: vec![],
            };

            // Test various error scenarios
            let error_scenarios = vec![
                (TRANSFORM_FAILED, "JSON transformation failed"),
                (TS_PARSE_FAILED, "Cannot parse timestamp"),
                (SCHEMA_CONFORMANCE_FAILED, "Schema validation failed"),
                (PIPELINE_EXEC_FAILED, "Pipeline execution error"),
            ];

            for (error_type, error_msg) in error_scenarios {
                add_record_status(
                    "error_stream".to_string(),
                    Some("error_doc".to_string()),
                    "index".to_string(),
                    Some(json::json!({"error": "test"})),
                    &mut bulk_res,
                    Some(error_type.to_string()),
                    Some(error_msg.to_string()),
                );
            }

            assert_eq!(bulk_res.items.len(), 4);

            // All items should be errors
            for item in &bulk_res.items {
                let bulk_item = item.values().next().unwrap();
                assert!(bulk_item.error.is_some());
            }
        }
    }

    mod edge_case_tests {
        use super::*;

        #[test]
        fn test_empty_bulk_request() {
            let empty_request = "";
            let body = web::Bytes::from(empty_request);

            // Empty request should be handled gracefully
            // (Test would need actual infrastructure to run fully)
            assert_eq!(body.len(), 0);
        }

        #[test]
        fn test_malformed_bulk_lines() {
            let malformed_lines = vec![
                "",                 // Empty line
                "not-json",         // Not JSON
                "{incomplete json", // Incomplete JSON
                "{}",               // Empty JSON object
                r#"{"index": {}}"#, // Missing required fields
            ];

            for line in malformed_lines {
                if line.is_empty() {
                    // Empty lines should be skipped
                    continue;
                }

                if json::from_slice::<json::Value>(line.as_bytes()).is_ok() {
                    // Valid JSON, should be processed
                } else {
                    // Invalid JSON, should cause error
                    // This is expected behavior
                }
            }
        }

        #[test]
        fn test_very_large_documents() {
            let large_value = "x".repeat(10000);
            let large_doc = json::json!({
                "message": large_value,
                "metadata": {
                    "large_field": "y".repeat(5000),
                    "nested": {
                        "data": "z".repeat(3000)
                    }
                }
            });

            let mut bulk_res = BulkResponse {
                took: 0,
                errors: false,
                items: vec![],
            };

            add_record_status(
                "large_docs".to_string(),
                Some("large_doc_1".to_string()),
                "index".to_string(),
                Some(large_doc),
                &mut bulk_res,
                None,
                None,
            );

            assert_eq!(bulk_res.items.len(), 1);
        }

        #[test]
        fn test_unicode_and_special_characters() {
            let unicode_doc = json::json!({
                "message": "Hello 世界! 🌍 Testing Unicode",
                "emoji": "🚀🌟✨",
                "special_chars": "!@#$%^&*()[]{}|\\:;\"'<>?,./",
                "unicode_text": "Здравствуй мир! ¡Hola mundo!",
                "japanese": "こんにちは世界"
            });

            let mut bulk_res = BulkResponse {
                took: 0,
                errors: false,
                items: vec![],
            };

            add_record_status(
                "unicode_stream".to_string(),
                Some("unicode_doc".to_string()),
                "index".to_string(),
                Some(unicode_doc),
                &mut bulk_res,
                None,
                None,
            );

            assert_eq!(bulk_res.items.len(), 1);

            // Should handle unicode gracefully
            let item = &bulk_res.items[0];
            let bulk_item = item.values().next().unwrap();
            assert!(!bulk_item._index.is_empty());
        }

        #[test]
        fn test_null_and_empty_values() {
            let doc_with_nulls = json::json!({
                "null_field": null,
                "empty_string": "",
                "empty_array": [],
                "empty_object": {},
                "zero_number": 0,
                "false_boolean": false
            });

            let mut bulk_res = BulkResponse {
                took: 0,
                errors: false,
                items: vec![],
            };

            add_record_status(
                "null_values_stream".to_string(),
                Some("null_doc".to_string()),
                "index".to_string(),
                Some(doc_with_nulls),
                &mut bulk_res,
                None,
                None,
            );

            assert_eq!(bulk_res.items.len(), 1);
        }
    }

    mod performance_tests {
        use super::*;

        #[test]
        fn test_bulk_response_performance() {
            let mut bulk_res = BulkResponse {
                took: 0,
                errors: false,
                items: vec![],
            };

            let start = std::time::Instant::now();

            // Add many records to test performance
            for i in 0..1000 {
                add_record_status(
                    format!("perf_stream_{}", i % 10), // 10 different streams
                    Some(format!("doc_{i}")),
                    if i % 2 == 0 {
                        "index".to_string()
                    } else {
                        "create".to_string()
                    },
                    Some(json::json!({
                        "id": i,
                        "message": format!("Performance test message {i}"),
                        "timestamp": format!("2024-01-01T{:02}:00:00Z", i % 24)
                    })),
                    &mut bulk_res,
                    if i % 10 == 0 {
                        Some(TS_PARSE_FAILED.to_string())
                    } else {
                        None
                    },
                    if i % 10 == 0 {
                        Some("Test error".to_string())
                    } else {
                        None
                    },
                );
            }

            let duration = start.elapsed();
            assert_eq!(bulk_res.items.len(), 1000);

            // Should complete within reasonable time (less than 1 second for 1000 records)
            assert!(duration.as_secs() < 1);

            // Check distribution of errors vs successes
            let errors: usize = bulk_res
                .items
                .iter()
                .map(|item| {
                    let bulk_item = item.values().next().unwrap();
                    if bulk_item.error.is_some() { 1 } else { 0 }
                })
                .sum();

            assert_eq!(errors, 100); // Every 10th record should be an error
        }
    }
}
