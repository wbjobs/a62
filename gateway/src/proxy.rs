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
use crate::timeout_budget::TimeoutBudget;
use crate::traffic_coloring::propagate_canary_headers;

pub async fn proxy_handler(
    State(state): State<Arc<AppState>>,
    Path(service_name): Path<String>,
    mut req: Request<Body>,
) -> Result<Response<Body>, StatusCode> {
    let service_config = state
        .get_service(&service_name)
        .ok_or(StatusCode::NOT_FOUND)?;

    let (budget, _from_header) =
        TimeoutBudget::from_header_or_new(req.headers(), state.global_timeout_ms());

    if budget.is_expired() {
        tracing::warn!("Timeout budget exhausted before routing");
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }

    if !budget.has_enough_budget(service_config.timeout()) {
        tracing::warn!(
            "Insufficient timeout budget: remaining={}ms, required={}ms",
            budget.remaining_ms(),
            service_config.timeout_ms
        );
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }

    let cb = state
        .get_circuit_breaker(&service_name)
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    if !cb.allow_request() {
        tracing::warn!("Circuit breaker open for service: {}", service_name);
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }

    let route_target =
        state
            .traffic_colorer
            .determine_route(&service_name, req.headers(), &service_config);

    tracing::debug!(
        "Routing to service={}, cluster={}, endpoint={}",
        service_name,
        route_target.cluster_type,
        route_target.endpoint
    );

    let headers = req.headers_mut();
    state
        .traffic_colorer
        .inject_canary_headers(headers, route_target.cluster_type);
    budget.inject_header(headers);

    let uri_path = req.uri().path_and_query().map(|p| p.as_str()).unwrap_or("/");
    let target_url = format!("{}{}", route_target.endpoint, uri_path);

    let method = req.method().clone();
    let mut proxy_headers = HeaderMap::new();
    propagate_canary_headers(req.headers(), &mut proxy_headers);
    budget.inject_header(&mut proxy_headers);

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

    let remaining_budget = budget.remaining();
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
                    "Backend error for service {}: status={}, duration={:?}",
                    service_name,
                    status,
                    duration
                );
            } else {
                cb.record_success(duration);
                tracing::debug!(
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
                tracing::error!("Failed to read response body: {}", e);
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
                tracing::warn!("Request to {} timed out after {:?}", service_name, duration);
                Err(StatusCode::GATEWAY_TIMEOUT)
            } else {
                tracing::error!("Request to {} failed: {}", service_name, e);
                Err(StatusCode::BAD_GATEWAY)
            }
        }
    }
}
