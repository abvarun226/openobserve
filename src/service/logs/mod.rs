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

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};

use arrow_schema::{DataType, Field};
use bulk::SCHEMA_CONFORMANCE_FAILED;
use config::{
    DISTINCT_FIELDS, META_ORG_ID, SIZE_IN_MB, get_config,
    meta::{
        alerts::alert::Alert,
        self_reporting::usage::{RequestStats, UsageType},
        stream::{PartitionTimeLevel, StreamParams, StreamPartition, StreamType},
    },
    metrics,
    utils::{
        json::{Map, Value, get_string_value},
        schema_ext::SchemaExt,
        time::now_micros,
    },
};
use infra::{
    errors::{Error, Result},
    schema::{SchemaCache, unwrap_partition_time_level},
};

#[cfg(feature = "cloud")]
use crate::service::stream::get_stream;
use crate::{
    common::meta::{ingestion::IngestionStatus, stream::SchemaRecords},
    service::{
        alerts::alert::AlertExt,
        db,
        ingestion::{TriggerAlertData, evaluate_trigger, get_write_partition_key, write_file},
        metadata::{
            MetadataItem, MetadataType,
            distinct_values::{DISTINCT_STREAM_PREFIX, DvItem},
            write,
        },
        schema::{check_for_schema, stream_schema_exists},
        self_reporting::report_request_usage_stats,
    },
};

pub mod bulk;
pub mod hec;
pub mod ingest;
pub mod loki;
pub mod otlp;
pub mod patterns;

static BULK_OPERATORS: [&str; 3] = ["create", "index", "update"];

pub type O2IngestJsonData = (Vec<(i64, Map<String, Value>)>, Option<usize>);

fn parse_bulk_index(v: &Value) -> Option<(&str, &str, Option<&str>)> {
    let local_val = v.as_object().unwrap();
    for action in BULK_OPERATORS {
        if let Some(val) = local_val.get(action) {
            let Some(local_val) = val.as_object() else {
                log::warn!("Invalid bulk index action: {action}");
                continue;
            };
            let Some(index) = local_val.get("_index").and_then(|v| v.as_str()) else {
                continue;
            };
            let doc_id = local_val.get("_id").and_then(|v| v.as_str());
            return Some((action, index, doc_id));
        };
    }
    None
}

/// Fast extraction of action, _index, and _id from a bulk metadata line's raw bytes.
/// Returns (action, index, doc_id) without full JSON parsing.
/// Falls back to None on any unexpected format.
fn parse_bulk_index_fast(line: &[u8]) -> Option<(&str, &str, Option<&str>)> {
    // Expected format: {"index":{"_index":"name"}} or {"create":{"_index":"name","_id":"id"}}
    // Find the action by looking for the first '"' after '{'
    let s = std::str::from_utf8(line).ok()?;
    let s = s.trim();
    if !s.starts_with('{') || !s.ends_with('}') {
        return None;
    }

    // Find action: first quoted key
    let first_quote = s.find('"')? + 1;
    let action_end = first_quote + s[first_quote..].find('"')?;
    let action = &s[first_quote..action_end];

    // Validate action
    if !BULK_OPERATORS.contains(&action) {
        return None;
    }

    // Find _index value
    let idx_marker = "\"_index\":\"";
    let idx_start = s.find(idx_marker)? + idx_marker.len();
    let idx_end = idx_start + s[idx_start..].find('"')?;
    let index = &s[idx_start..idx_end];

    // Find _id value (optional)
    let doc_id = {
        let id_marker = "\"_id\":\"";
        s.find(id_marker).and_then(|pos| {
            let id_start = pos + id_marker.len();
            let id_end = id_start + s[id_start..].find('"')?;
            Some(&s[id_start..id_end])
        })
    };

    Some((action, index, doc_id))
}

pub fn cast_to_type(
    value: &mut Map<String, Value>,
    delta: &[Field],
) -> Result<(), anyhow::Error> {
    let mut parse_error = String::new();
    for field in delta {
        let field_name = field.name();
        let Some(val) = value.get(field_name) else {
            continue;
        };
        if val.is_null() {
            value.insert(field_name.clone(), Value::Null);
            continue;
        }
        match field.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => {
                if val.is_string() {
                    continue;
                }
                value.insert(field_name.clone(), Value::String(get_string_value(val)));
            }
            DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8 => {
                let ret = match val {
                    Value::Number(_) => {
                        continue;
                    }
                    Value::String(v) => v.parse::<i64>().map_err(|e| e.to_string()),
                    Value::Bool(v) => Ok(if *v { 1 } else { 0 }),
                    _ => Err("".to_string()),
                };
                match ret {
                    Ok(val) => {
                        value.insert(field_name.clone(), Value::Number(val.into()));
                    }
                    Err(_) => set_parsing_error(&mut parse_error, field),
                };
            }
            DataType::UInt64 | DataType::UInt32 | DataType::UInt16 | DataType::UInt8 => {
                let ret = match val {
                    Value::Number(_) => {
                        continue;
                    }
                    Value::String(v) => v.parse::<u64>().map_err(|e| e.to_string()),
                    Value::Bool(v) => Ok(if *v { 1 } else { 0 }),
                    _ => Err("".to_string()),
                };
                match ret {
                    Ok(val) => {
                        value.insert(field_name.clone(), Value::Number(val.into()));
                    }
                    Err(_) => set_parsing_error(&mut parse_error, field),
                };
            }
            DataType::Float64 | DataType::Float32 | DataType::Float16 => {
                let ret = match val {
                    Value::Number(_) => {
                        continue;
                    }
                    Value::String(v) => v.parse::<f64>().map_err(|e| e.to_string()),
                    Value::Bool(v) => Ok(if *v { 1.0 } else { 0.0 }),
                    _ => Err("".to_string()),
                };
                match ret {
                    Ok(val) => {
                        value.insert(
                            field_name.clone(),
                            Value::Number(serde_json::Number::from_f64(val).unwrap()),
                        );
                    }
                    Err(_) => set_parsing_error(&mut parse_error, field),
                };
            }
            DataType::Boolean => {
                let ret = match val {
                    Value::Bool(_) => {
                        continue;
                    }
                    Value::Number(v) => Ok(v.as_f64().unwrap_or(0.0) > 0.0),
                    Value::String(v) => v.parse::<bool>().map_err(|e| e.to_string()),
                    _ => Err("".to_string()),
                };
                match ret {
                    Ok(val) => {
                        value.insert(field_name.clone(), Value::Bool(val));
                    }
                    Err(_) => set_parsing_error(&mut parse_error, field),
                };
            }
            _ => set_parsing_error(&mut parse_error, field),
        };
    }
    if !parse_error.is_empty() {
        Err(anyhow::Error::msg(parse_error))
    } else {
        Ok(())
    }
}

fn set_parsing_error(parse_error: &mut String, field: &Field) {
    parse_error.push_str(&format!(
        "Failed to cast {} to type {} ",
        field.name(),
        field.data_type()
    ));
}

#[allow(clippy::too_many_arguments)]
async fn write_logs_by_stream(
    thread_id: usize,
    org_id: &str,
    user_email: &str,
    time_stats: (i64, &Instant), // started_at
    usage_type: UsageType,
    status: &mut IngestionStatus,
    json_data_by_stream: HashMap<String, O2IngestJsonData>,
    byte_size_by_stream: HashMap<String, usize>,
    derived_streams: HashSet<String>,
) -> Result<()> {
    for (stream_name, (json_data, fn_num)) in json_data_by_stream {
        // check if we are allowed to ingest
        if db::compact::retention::is_deleting_stream(org_id, StreamType::Logs, &stream_name, None)
        {
            log::warn!("stream [{stream_name}] is being deleted");
            continue; // skip
        }

        // for cloud, we want to sent event when user creates a new stream
        #[cfg(feature = "cloud")]
        if get_stream(org_id, &stream_name, StreamType::Logs)
            .await
            .is_none()
        {
            let org = match super::organization::get_org(org_id).await {
                None => {
                    return Err(Error::Message(format!(
                        "org with id {org_id} not found in db"
                    )));
                }
                Some(org) => org,
            };

            super::self_reporting::cloud_events::enqueue_cloud_event(
                super::self_reporting::cloud_events::CloudEvent {
                    org_id: org.identifier.clone(),
                    org_name: org.name.clone(),
                    org_type: org.org_type.clone(),
                    user: Some(user_email.to_string()),
                    event: super::self_reporting::cloud_events::EventType::StreamCreated,
                    subscription_type: None,
                    stream_name: Some(stream_name.clone()),
                },
            )
            .await;
        }

        // write json data by stream
        let mut req_stats = write_logs(
            thread_id,
            org_id,
            &stream_name,
            status,
            json_data,
            derived_streams.contains(&stream_name),
        )
        .await?;

        let time_took = time_stats.1.elapsed().as_secs_f64();
        req_stats.response_time = time_took;
        req_stats.user_email = if user_email.is_empty() {
            None
        } else {
            Some(user_email.to_string())
        };

        req_stats.dropped_records = match status {
            IngestionStatus::Record(s) => s.failed.into(),
            IngestionStatus::Bulk(s) => {
                if s.errors {
                    s.items
                        .iter()
                        .map(|i| {
                            i.values()
                                .map(|res| if res.error.is_some() { 1 } else { 0 })
                                .sum::<i64>()
                        })
                        .sum()
                } else {
                    0
                }
            }
        };

        if let Some(fns_length) = fn_num {
            // the issue here is req_stats.size calculates size after flattening and
            // adding _timestamp col etc ; which inflates the size compared to the actual
            // data sent by user. So when reporting we check if the calling function has provided us
            // an "actual" size of the input, and is so use that instead of the req_stats
            if let Some(size) = byte_size_by_stream.get(&stream_name) {
                // req_stats already divides the size in mb
                req_stats.size = *size as f64 / SIZE_IN_MB;
            }
            report_request_usage_stats(
                req_stats,
                org_id,
                &stream_name,
                StreamType::Logs,
                usage_type,
                fns_length as u16,
                time_stats.0,
            )
            .await;
        }
    }
    Ok(())
}

pub(crate) async fn write_logs(
    thread_id: usize,
    org_id: &str,
    stream_name: &str,
    status: &mut IngestionStatus,
    json_data: Vec<(i64, Map<String, Value>)>,
    is_derived: bool,
) -> Result<RequestStats> {
    let cfg = get_config();
    let log_ingest_errors = ingestion_log_enabled().await;
    // get schema and stream settings
    let mut stream_schema_map: HashMap<String, SchemaCache> = HashMap::new();
    let stream_schema = stream_schema_exists(
        org_id,
        stream_name,
        StreamType::Logs,
        &mut stream_schema_map,
    )
    .await;

    let schema = match stream_schema_map.get(stream_name) {
        Some(schema) => schema.schema().clone(),
        None => {
            return Err(Error::IngestionError(format!(
                "Schema not found for stream: {stream_name}"
            )));
        }
    };
    let stream_settings = infra::schema::unwrap_stream_settings(&schema).unwrap_or_default();

    let mut partition_keys: Vec<StreamPartition> = vec![];
    let mut partition_time_level = PartitionTimeLevel::from(cfg.limit.logs_file_retention.as_str());
    if stream_schema.has_partition_keys {
        partition_keys = stream_settings.partition_keys;
        partition_time_level =
            unwrap_partition_time_level(stream_settings.partition_time_level, StreamType::Logs);
    }

    // Start get stream alerts
    let mut stream_alerts_map: HashMap<String, Vec<Alert>> = HashMap::new();
    crate::service::ingestion::get_stream_alerts(
        &[StreamParams {
            org_id: org_id.to_owned().into(),
            stream_name: stream_name.to_owned().into(),
            stream_type: StreamType::Logs,
        }],
        &mut stream_alerts_map,
    )
    .await;
    let cur_stream_alerts =
        stream_alerts_map.get(&format!("{}/{}/{}", org_id, StreamType::Logs, stream_name));
    let mut triggers: TriggerAlertData =
        Vec::with_capacity(cur_stream_alerts.map_or(0, |v| v.len()));
    let mut evaluated_alerts = HashSet::new();
    // End get stream alert

    // start check for schema
    let min_timestamp = json_data[0].0;
    let (schema_evolution, infer_schema) = check_for_schema(
        org_id,
        stream_name,
        StreamType::Logs,
        &mut stream_schema_map,
        json_data.iter().map(|(_, v)| v).collect(),
        min_timestamp,
        is_derived,
    )
    .await?;

    // get schema
    let latest_schema = stream_schema_map
        .get(stream_name)
        .unwrap()
        .schema()
        .as_ref()
        .clone()
        .with_metadata(HashMap::new());
    let schema_key = latest_schema.hash_key();
    // use latest schema as schema key
    // use inferred schema as record schema
    let rec_schema = match infer_schema {
        // use latest_schema's datetype for record schema
        Some(schema) => Arc::new(schema.cloned_from(&latest_schema)),
        None => Arc::new(latest_schema),
    };

    let mut distinct_values = Vec::with_capacity(16);

    let mut write_buf: HashMap<String, SchemaRecords> = HashMap::new();
    // Cache partition key: (bucket_id, key_string) to skip recomputation
    let mut cached_partition: Option<(i64, String)> = None;

    // Pre-compute the cast delta once, outside the per-record loop
    let cast_delta: Option<Vec<Field>> = schema_evolution.types_delta.as_ref().map(|delta| {
        if !schema_evolution.is_schema_changed {
            delta.clone()
        } else {
            delta
                .iter()
                .filter(|x| x.metadata().contains_key("zo_cast"))
                .cloned()
                .collect()
        }
    });

    let batch_has_doc_id = !cfg.common.bulk_api_response_errors_only
        && json_data
            .iter()
            .any(|(_, record)| record.contains_key("_id"));

    // Bulk fast path: skip per-record loop when no partition keys, no cast,
    // no doc_id, no alerts, no distinct values. Build SchemaRecords directly.
    let can_skip_loop = partition_keys.is_empty()
        && cast_delta.is_none()
        && !batch_has_doc_id
        && !stream_settings.enable_distinct_fields;
    // Check if all records fall in the same partition bucket
    let single_bucket = if can_skip_loop && !json_data.is_empty() {
        let divisor = if matches!(partition_time_level, PartitionTimeLevel::Daily) {
            86_400_000_000i64
        } else {
            3_600_000_000i64
        };
        let first_bucket = json_data[0].0 / divisor;
        json_data.iter().all(|(ts, _)| ts / divisor == first_bucket)
    } else {
        false
    };
    if single_bucket {
        // Fast path: all records in one partition. Build SchemaRecords
        // directly without per-record iteration.
        let (first_ts, ref first_rec) = json_data[0];
        let hour_key = get_write_partition_key(
            first_ts, &partition_keys, partition_time_level,
            first_rec, Some(&schema_key),
        );
        let records: Vec<Value> = json_data
            .into_iter()
            .map(|(_, map)| Value::Object(map))
            .collect();
        let record_count = records.len();
        write_buf.insert(hour_key, SchemaRecords {
            schema_key: schema_key.clone(),
            schema: rec_schema.clone(),
            records_size: 0,
            records,
        });
        // Generate bulk response success items for the fast path.
        match status {
            IngestionStatus::Record(status) => {
                status.successful += record_count as u32;
            }
            IngestionStatus::Bulk(bulk_res) => {
                for _ in 0..record_count {
                    bulk::add_record_status(
                        stream_name.to_string(),
                        None,
                        "".to_string(),
                        None,
                        bulk_res,
                        None,
                        None,
                    );
                }
            }
        }
    } else {

    for (timestamp, mut record_val) in json_data {
        let doc_id = if batch_has_doc_id {
            record_val
                .get("_id")
                .map(|v| v.as_str().unwrap().to_string())
        } else {
            None
        };

        // validate record
        if let Some(delta) = cast_delta.as_ref() {
            let ret_val = if !delta.is_empty() {
                cast_to_type(&mut record_val, delta)
            } else {
                Ok(())
            };
            if let Err(e) = ret_val {
                // update status(fail)
                match status {
                    IngestionStatus::Record(status) => {
                        status.failed += 1;
                        status.error = e.to_string();
                        metrics::INGEST_ERRORS
                            .with_label_values(&[
                                org_id,
                                StreamType::Logs.as_str(),
                                stream_name,
                                SCHEMA_CONFORMANCE_FAILED,
                            ])
                            .inc();
                        log_failed_record(log_ingest_errors, &record_val, &e.to_string());
                    }
                    IngestionStatus::Bulk(bulk_res) => {
                        bulk_res.errors = true;
                        metrics::INGEST_ERRORS
                            .with_label_values(&[
                                org_id,
                                StreamType::Logs.as_str(),
                                stream_name,
                                SCHEMA_CONFORMANCE_FAILED,
                            ])
                            .inc();
                        log_failed_record(log_ingest_errors, &record_val, &e.to_string());
                        bulk::add_record_status(
                            stream_name.to_string(),
                            doc_id,
                            "".to_string(),
                            Some(Value::Object(record_val.clone())),
                            bulk_res,
                            Some(bulk::SCHEMA_CONFORMANCE_FAILED.to_string()),
                            Some(e.to_string()),
                        );
                    }
                }
                continue;
            }
        }

        // start check for alert trigger
        if let Some(alerts) = cur_stream_alerts
            && triggers.len() < alerts.len()
        {
            let end_time = now_micros();
            for alert in alerts {
                let key = format!(
                    "{}/{}/{}/{}",
                    org_id,
                    StreamType::Logs,
                    alert.stream_name,
                    alert.get_unique_key()
                );
                // For one alert, only one trigger per request
                // Trigger for this alert is already added.
                if evaluated_alerts.contains(&key) {
                    continue;
                }
                match alert
                    .evaluate(Some(&record_val), (None, end_time), None)
                    .await
                {
                    Ok(trigger_results) if trigger_results.data.is_some() => {
                        triggers.push((alert.clone(), trigger_results.data.unwrap()));
                        evaluated_alerts.insert(key);
                    }
                    Ok(_) => {
                        // the data doesn't satisfy the alert condition
                    }
                    Err(e) => {
                        log::error!("[LOGS] Error while evaluating realtime alert: {e}");
                    }
                }
            }
        }
        // end check for alert triggers

        // get distinct_value items
        if stream_settings.enable_distinct_fields {
            let mut map = Map::new();
            for field in DISTINCT_FIELDS.iter().chain(
                stream_settings
                    .distinct_value_fields
                    .iter()
                    .map(|f| &f.name),
            ) {
                if let Some(val) = record_val.get(field) {
                    map.insert(field.clone(), val.clone());
                }
            }

            if !map.is_empty() {
                // add distinct values
                distinct_values.push(MetadataItem::DistinctValues(DvItem {
                    stream_type: StreamType::Logs,
                    stream_name: stream_name.to_string(),
                    value: map,
                }));
            }
        }

        // get hour key — reuse cached key when timestamp falls in same bucket
        let bucket = if matches!(partition_time_level, PartitionTimeLevel::Daily) {
            timestamp / 86_400_000_000
        } else {
            timestamp / 3_600_000_000
        };
        let hour_key = if partition_keys.is_empty() {
            if let Some((cached_bucket, ref key)) = cached_partition {
                if cached_bucket == bucket {
                    key.clone()
                } else {
                    let k = get_write_partition_key(
                        timestamp, &partition_keys, partition_time_level,
                        &record_val, Some(&schema_key),
                    );
                    cached_partition = Some((bucket, k.clone()));
                    k
                }
            } else {
                let k = get_write_partition_key(
                    timestamp, &partition_keys, partition_time_level,
                    &record_val, Some(&schema_key),
                );
                cached_partition = Some((bucket, k.clone()));
                k
            }
        } else {
            get_write_partition_key(
                timestamp, &partition_keys, partition_time_level,
                &record_val, Some(&schema_key),
            )
        };

        let hour_buf = write_buf.entry(hour_key).or_insert_with(|| SchemaRecords {
            schema_key: schema_key.clone(),
            schema: rec_schema.clone(),
            records: vec![],
            records_size: 0,
        });
        let record_val = Value::Object(record_val);
        hour_buf.records.push(record_val);
        // records_size is a pre-allocation hint; into_bytes resets it to the
        // actual serialized length. Skip the per-record estimate_json_bytes
        // walk to reduce CPU cost in the hot loop.

        // update status(success)
        match status {
            IngestionStatus::Record(status) => {
                status.successful += 1;
            }
            IngestionStatus::Bulk(bulk_res) => {
                bulk::add_record_status(
                    stream_name.to_string(),
                    doc_id,
                    "".to_string(),
                    None,
                    bulk_res,
                    None,
                    None,
                );
            }
        }
    }

    } // end else (slow path with per-record loop)

    // write data to wal
    let writer =
        ingester::get_writer(thread_id, org_id, StreamType::Logs.as_str(), stream_name).await;
    let req_stats = write_file(
        &writer,
        org_id,
        stream_name,
        write_buf,
        !cfg.common.wal_fsync_disabled,
    )
    .await?;

    // send distinct_values
    if !distinct_values.is_empty()
        && !stream_name.starts_with(DISTINCT_STREAM_PREFIX)
        && stream_settings.enable_distinct_fields
        && let Err(e) = write(org_id, MetadataType::DistinctValues, distinct_values).await
    {
        log::error!("Error while writing distinct values: {e}");
    }

    // only one trigger per request
    if !triggers.is_empty() {
        tokio::spawn(evaluate_trigger(triggers));
    }

    Ok(req_stats)
}

async fn ingestion_log_enabled() -> bool {
    if !get_config().common.ingestion_log_enabled {
        return false;
    }
    // the logging will be enabled through meta only
    db::organization::get_org_setting_toggle_ingestion_logs(META_ORG_ID)
        .await
        .unwrap_or(false)
}

fn log_failed_record<T: std::fmt::Debug>(enabled: bool, record: &T, error: &str) {
    if !enabled {
        return;
    }
    log::warn!("failed to process record with error {error} : {record:?} ");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_parsing_error() {
        let mut parse_error = String::new();
        set_parsing_error(&mut parse_error, &Field::new("test", DataType::Utf8, true));
        assert!(!parse_error.is_empty());
    }

    #[test]
    fn test_cast_to_type() {
        let mut local_val = Map::new();
        local_val.insert("test".to_string(), Value::from("test13212"));
        let delta = vec![Field::new("test", DataType::Utf8, true)];
        let ret_val = cast_to_type(&mut local_val, &delta);
        assert!(ret_val.is_ok());
    }
}
