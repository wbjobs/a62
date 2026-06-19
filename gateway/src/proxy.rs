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
use crate::traffic_coloring::propagate_canary_headers;

pub async fn proxy_handler(
    State(state): State<Arc<AppState>>,
    Path(service_name): Path<String>,
    req: Request<Body>,
) -> Result<Response<Body>, StatusCode> {
    let _service_config = state
        .get_service(&service_name)
        .ok_or(StatusCode::NOT_FOUND)?;

    let span = SpanContext::from_headers(req.headers(), state.global_timeout_ms());

    with_span_context(span, || async move {
        proxy_inner(state, service_name, req).await
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

    if span.is_budget_expired() {
        tracing::warn!(
            request_id = %span.request_id,
            "Timeout budget exhausted before routing"
        );
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }

    if !span.has_enough_budget(service_config.timeout()) {
        tracing::warn!(
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

    let uri_path = req.uri().path_and_query().map(|p| p.as_str()).unwrap_or("/");
    let target_url = format!("{}{}", route_target.endpoint, uri_path);

    let method = req.method().clone();
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

    let start_time = Instant::now();

    let remaining_budget = span.remaining_budget();
    let request_timeout = std::cmp::min(service_config.timeout(), remaining_budget);

    let proxy_request = state
        .http_client
        .request(method, &target_url)
        .headers(proxy_headers)
        .timeout(request_timeout)
        .body(body_bytes);

    let result = proxy_request.send().await;

    let duration = start_time.elapsed();

    match result {
        Ok(resp) => {
            let status = resp.status();

            if status.is_server_error() {
                cb.record_error(duration);
                tracing::warn!(
                    request_id = %span.request_id,
                    "Backend error for service {}: status={}, duration={:?}",
                    service_name,
                    status,
                    duration
                );
            } else {
                cb.record_success(duration);
                tracing::debug!(
                    request_id = %span.request_id,
                    "Request to {} succeeded: status={}, duration={:?}",
                    service_name,
                    status,
                    duration
                );
            }

            let mut builder = Response::builder().status(status);

            for (key, value) in resp.headers().iter() {
                builder = builder.header(key.clone(), value.clone());
            }

            let body = resp.bytes().await.map_err(|e| {
                tracing::error!(
                    request_id = %span.request_id,
                    "Failed to read response body: {}",
                    e
                );
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

            let response = builder
                .body(Body::from(body))
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            Ok(response)
        }
        Err(e) => {
            cb.record_error(duration);

            if e.is_timeout() {
                tracing::warn!(
                    request_id = %span.request_id,
                    "Request to {} timed out after {:?}",
                    service_name,
                    duration
                );
                Err(StatusCode::GATEWAY_TIMEOUT)
            } else {
                tracing::error!(
                    request_id = %span.request_id,
                    "Request to {} failed: {}",
                    service_name,
                    e
                );
                Err(StatusCode::BAD_GATEWAY)
            }
        }
    }
}
