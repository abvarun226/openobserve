use super::db;
use crate::meta::alert::Trigger;
use crate::meta::http::HttpResponse as MetaHttpResponse;
use actix_web::{
    http::{self, StatusCode},
    HttpResponse,
};
use std::io::Error;
use tracing::info_span;

pub async fn save_trigger(alert_name: String, trigger: Trigger) -> Result<HttpResponse, Error> {
    let loc_span = info_span!("service:triggers:save");
    let _guard = loc_span.enter();

    db::triggers::set(&alert_name, trigger).await.unwrap();
    Ok(HttpResponse::Ok().json(MetaHttpResponse::message(
        http::StatusCode::OK.into(),
        "Trigger saved".to_string(),
    )))
}

pub async fn delete_trigger(alert_name: String) -> Result<HttpResponse, Error> {
    let loc_span = info_span!("service:trigger:delete");
    let _guard = loc_span.enter();
    let result = db::triggers::delete(&alert_name).await;
    match result {
        Ok(_) => Ok(HttpResponse::Ok().json(MetaHttpResponse::message(
            http::StatusCode::OK.into(),
            "Trigger deleted ".to_string(),
        ))),
        Err(err) => Ok(HttpResponse::NotFound().json(MetaHttpResponse::error(
            StatusCode::NOT_FOUND.into(),
            Some(err.to_string()),
        ))),
    }
}

pub async fn get_alert(
    org_id: String,
    stream_name: String,
    name: String,
) -> Result<HttpResponse, Error> {
    let loc_span = info_span!("service:alerts:get");
    let _guard = loc_span.enter();
    let result = db::alerts::get(org_id.as_str(), stream_name.as_str(), name.as_str()).await;
    match result {
        Ok(alert) => Ok(HttpResponse::Ok().json(alert)),
        Err(_) => Ok(HttpResponse::NotFound().json(MetaHttpResponse::error(
            StatusCode::NOT_FOUND.into(),
            Some("alert not found".to_string()),
        ))),
    }
}
