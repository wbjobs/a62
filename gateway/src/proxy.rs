use std::sync::Arc;
use std::time::Instant;

use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, Request, StatusCode},
    response::Response,
};
use http_body_util::BodyExt;

use crate::state::AppState;
use crate::span_context::{SpanContext, with_span_context};
use crate::trace_sampler::TraceSampler;
use crate::traffic_coloring::propagate_canary_headers;
use crate::trace_record::{DownstreamCall, TraceRecord, cluster_type_to_string, sampling_reason_to_string};

pub async fn proxy_handler(
    State(state): State<Arc<AppState>>,
    Path(service_name): Path<String>,
    req: Request<Body>,
) -> Result<Response<Body>, StatusCode> {
    let _service_config = state
        .get_service(&service_name)
        .ok_or(StatusCode::NOT_FOUND)?;

    let method = req.method().clone();
    let url = req
        .uri()
        .path_and_query()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| "/".to_string());

    let sampling_decision = state.sampler.should_sample(req.headers(), &url);

    let mut span = SpanContext::from_headers(req.headers(), state.global_timeout_ms());
    span = span.with_sampling(
        sampling_decision.sampled,
        sampling_decision.reason,
        sampling_decision.trace_id,
    );

    let is_sampled = span.sampled;
    let span_clone_for_record = span.clone();
    let method_for_record = method.clone();
    let url_for_record = url.clone();
    let req_headers_for_record = req.headers().clone();
    let max_body_size = state.max_body_size;
    let logger = state.logger.clone();

    with_span_context(span, || async move {
        let result = proxy_inner(state.clone(), service_name.clone(), req).await;

        if is_sampled {
            let mut record = TraceRecord::new(
                span_clone_for_record.trace_id.clone(),
                span_clone_for_record.request_id.clone(),
            );
            record.trace_id = span_clone_for_record.trace_id;
            record.sampled = true;
            record.sampling_reason = sampling_reason_to_string(sampling_decision.reason);
            record.is_canary = span_clone_for_record.is_canary;
            record.cluster_type = cluster_type_to_string(span_clone_for_record.cluster_type);
            record.method = method_for_record.to_string();
            record.path = url_for_record.clone();
            record.with_headers(&req_headers_for_record, true);

            match &result {
                Ok(response) => {
                    record.status_code = response.status().as_u16();
                    record.with_headers(response.headers(), false);
                }
                Err(status) => {
                    record.status_code = status.as_u16();
                    record.error_message = Some(status.canonical_reason().unwrap_or("Unknown").to_string());
                }
            }

            record.finish();

            let _ = max_body_size;

            logger.log(record);
        }

        result
    })
    .await
}

async fn proxy_inner(
    state: Arc<AppState>,
    service_name: String,
    req: Request<Body>,
) -> Result<Response<Body>, StatusCode> {
    let service_config = state
        .get_service(&service_name)
        .ok_or(StatusCode::NOT_FOUND)?;

    let span = crate::span_context::get_span_context();
    let is_sampled = span.sampled;

    if span.is_budget_expired() {
        tracing::warn!(
            trace_id = %span.trace_id,
            request_id = %span.request_id,
            "Timeout budget exhausted before routing"
        );
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }

    if !span.has_enough_budget(service_config.timeout()) {
        tracing::warn!(
            trace_id = %span.trace_id,
            request_id = %span.request_id,
            "Insufficient timeout budget: remaining={}ms, required={}ms",
            span.remaining_budget_ms(),
            service_config.timeout_ms
        );
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }

    let cb = state
        .get_circuit_breaker(&service_name)
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    if !cb.allow_request() {
        tracing::warn!(
            trace_id = %span.trace_id,
            request_id = %span.request_id,
            "Circuit breaker open for service: {}",
            service_name
        );
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }

    let req_headers = req.headers().clone();
    let route_target =
        state
            .traffic_colorer
            .determine_route(&service_name, &req_headers, &service_config);

    tracing::debug!(
        trace_id = %span.trace_id,
        request_id = %span.request_id,
        "Routing to service={}, cluster={}, endpoint={}",
        service_name,
        route_target.cluster_type,
        route_target.endpoint
    );

    crate::span_context::update_span_context(|ctx| {
        ctx.set_cluster_type(route_target.cluster_type);
    });

    let mut req = req;
    let headers = req.headers_mut();
    state
        .traffic_colorer
        .inject_canary_headers(headers, route_target.cluster_type);
    let span = crate::span_context::get_span_context();
    span.inject_headers(headers);

    let method = req.method().clone();
    let uri_path = req
        .uri()
        .path_and_query()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| "/".to_string());
    let target_url = format!("{}{}", route_target.endpoint, uri_path);

    let mut proxy_headers = HeaderMap::new();
    propagate_canary_headers(req.headers(), &mut proxy_headers);
    span.inject_headers(&mut proxy_headers);

    for (key, value) in req.headers().iter() {
        if !proxy_headers.contains_key(key) {
            proxy_headers.insert(key.clone(), value.clone());
        }
    }

    let body_bytes = req
        .into_body()
        .collect()
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?
        .to_bytes();

    let request_body_for_trace = if is_sampled {
        Some(body_bytes.clone())
    } else {
        None
    };

    let start_time = Instant::now();

    let remaining_budget = span.remaining_budget();
    let request_timeout = std::cmp::min(service_config.timeout(), remaining_budget);

    let proxy_request = state
        .http_client
        .request(method.clone(), &target_url)
        .headers(proxy_headers.clone())
        .timeout(request_timeout)
        .body(body_bytes.clone());

    let result = proxy_request.send().await;

    let duration = start_time.elapsed();

    match result {
        Ok(resp) => {
            let status = resp.status();

            if status.is_server_error() {
                cb.record_error(duration);
                tracing::warn!(
                    trace_id = %span.trace_id,
                    request_id = %span.request_id,
                    "Backend error for service {}: status={}, duration={:?}",
                    service_name,
                    status,
                    duration
                );
            } else {
                cb.record_success(duration);
                tracing::debug!(
                    trace_id = %span.trace_id,
                    request_id = %span.request_id,
                    "Request to {} succeeded: status={}, duration={:?}",
                    service_name,
                    status,
                    duration
                );
            }

            let resp_headers = resp.headers().clone();
            let resp_body_bytes = resp.bytes().await.map_err(|e| {
                tracing::error!(
                    trace_id = %span.trace_id,
                    request_id = %span.request_id,
                    "Failed to read response body: {}",
                    e
                );
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

            if is_sampled {
                let downstream = DownstreamCall {
                    service_name: service_name.clone(),
                    cluster_type: route_target.cluster_type.to_string(),
                    endpoint: route_target.endpoint.clone(),
                    method: method.to_string(),
                    path: uri_path.to_string(),
                    status_code: status.as_u16(),
                    duration_ms: duration.as_millis() as u64,
                    is_error: status.is_server_error(),
                    is_timeout: false,
                    request_headers: header_map_to_hashmap(&proxy_headers),
                    response_headers: header_map_to_hashmap(&resp_headers),
                };

                crate::span_context::update_span_context(|_ctx| {
                    let _ = (request_body_for_trace.clone(), resp_body_bytes.clone(), downstream);
                });
            }

            let mut builder = Response::builder().status(status);
            for (key, value) in resp_headers.iter() {
                builder = builder.header(key.clone(), value.clone());
            }

            let response = builder
                .body(Body::from(resp_body_bytes))
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            Ok(response)
        }
        Err(e) => {
            cb.record_error(duration);
            let is_timeout = e.is_timeout();

            if is_timeout {
                tracing::warn!(
                    trace_id = %span.trace_id,
                    request_id = %span.request_id,
                    "Request to {} timed out after {:?}",
                    service_name,
                    duration
                );
            } else {
                tracing::error!(
                    trace_id = %span.trace_id,
                    request_id = %span.request_id,
                    "Request to {} failed: {}",
                    service_name,
                    e
                );
            }

            if is_sampled {
                let downstream = DownstreamCall {
                    service_name: service_name.clone(),
                    cluster_type: route_target.cluster_type.to_string(),
                    endpoint: route_target.endpoint.clone(),
                    method: method.to_string(),
                    path: uri_path.to_string(),
                    status_code: 0,
                    duration_ms: duration.as_millis() as u64,
                    is_error: true,
                    is_timeout,
                    request_headers: header_map_to_hashmap(&proxy_headers),
                    response_headers: std::collections::HashMap::new(),
                };

                crate::span_context::update_span_context(|_ctx| {
                    let _ = (request_body_for_trace.clone(), downstream);
                });
            }

            if is_timeout {
                Err(StatusCode::GATEWAY_TIMEOUT)
            } else {
                Err(StatusCode::BAD_GATEWAY)
            }
        }
    }
}

fn header_map_to_hashmap(headers: &HeaderMap) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    for (key, value) in headers.iter() {
        if let Ok(v) = value.to_str() {
            map.insert(key.as_str().to_string(), v.to_string());
        }
    }
    map
}
