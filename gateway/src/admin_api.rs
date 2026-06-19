use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};

use crate::config::ServiceConfig;
use crate::state::AppState;

#[derive(Debug, Serialize)]
pub struct ServiceStatus {
    pub name: String,
    pub circuit_state: String,
    pub total_requests: u64,
    pub error_count: u64,
    pub slow_call_count: u64,
    pub error_rate: f64,
    pub config: ServiceConfig,
}

#[derive(Debug, Deserialize)]
pub struct UpdateCircuitBreakerRequest {
    pub error_threshold: Option<f64>,
    pub request_volume_threshold: Option<u64>,
    pub sleep_window_ms: Option<u64>,
    pub slow_call_threshold_ms: Option<u64>,
    pub slow_call_ratio_threshold: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateServiceConfigRequest {
    pub timeout_ms: Option<u64>,
    pub canary_ratio: Option<f64>,
    pub stable_endpoints: Option<Vec<String>>,
    pub canary_endpoints: Option<Vec<String>>,
    pub circuit_breaker: Option<UpdateCircuitBreakerRequest>,
}

#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub message: Option<String>,
}

impl<T> ApiResponse<T> {
    fn success(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            message: None,
        }
    }

    fn error(message: &str) -> Self {
        Self {
            success: false,
            data: None,
            message: Some(message.to_string()),
        }
    }
}

pub async fn list_services(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let config = state.config.read();
    let service_names: Vec<String> = config.services.keys().cloned().collect();
    Json(ApiResponse::success(service_names))
}

pub async fn get_service_status(
    State(state): State<Arc<AppState>>,
    Path(service_name): Path<String>,
) -> impl IntoResponse {
    let service_config = state.get_service(&service_name);

    match service_config {
        Some(config) => {
            let cb = state.get_circuit_breaker(&service_name);
            let (total, errors, slow) = cb
                .as_ref()
                .map(|c| c.metrics())
                .unwrap_or((0, 0, 0));

            let circuit_state = cb
                .as_ref()
                .map(|c| format!("{:?}", c.state()))
                .unwrap_or("Unknown".to_string());

            let error_rate = if total > 0 {
                errors as f64 / total as f64
            } else {
                0.0
            };

            let status = ServiceStatus {
                name: service_name.clone(),
                circuit_state,
                total_requests: total,
                error_count: errors,
                slow_call_count: slow,
                error_rate,
                config,
            };

            (StatusCode::OK, Json(ApiResponse::success(status)))
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(ApiResponse::<ServiceStatus>::error("Service not found")),
        ),
    }
}

pub async fn update_service_config(
    State(state): State<Arc<AppState>>,
    Path(service_name): Path<String>,
    Json(payload): Json<UpdateServiceConfigRequest>,
) -> impl IntoResponse {
    let current_config = state.get_service(&service_name);

    match current_config {
        Some(mut config) => {
            if let Some(timeout_ms) = payload.timeout_ms {
                config.timeout_ms = timeout_ms;
            }
            if let Some(canary_ratio) = payload.canary_ratio {
                config.canary_ratio = canary_ratio;
            }
            if let Some(endpoints) = payload.stable_endpoints {
                config.stable_cluster.endpoints = endpoints;
            }
            if let Some(canary_endpoints) = payload.canary_endpoints {
                if config.canary_cluster.is_some() {
                    config.canary_cluster.as_mut().unwrap().endpoints = canary_endpoints;
                }
            }
            if let Some(cb_update) = payload.circuit_breaker {
                if let Some(v) = cb_update.error_threshold {
                    config.circuit_breaker.error_threshold = v;
                }
                if let Some(v) = cb_update.request_volume_threshold {
                    config.circuit_breaker.request_volume_threshold = v;
                }
                if let Some(v) = cb_update.sleep_window_ms {
                    config.circuit_breaker.sleep_window_ms = v;
                }
                if let Some(v) = cb_update.slow_call_threshold_ms {
                    config.circuit_breaker.slow_call_threshold_ms = v;
                }
                if let Some(v) = cb_update.slow_call_ratio_threshold {
                    config.circuit_breaker.slow_call_ratio_threshold = v;
                }
            }

            state.update_service_config(&service_name, config.clone());

            (
                StatusCode::OK,
                Json(ApiResponse::success(config)),
            )
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(ApiResponse::<ServiceConfig>::error("Service not found")),
        ),
    }
}

pub async fn reset_circuit_breaker(
    State(state): State<Arc<AppState>>,
    Path(service_name): Path<String>,
) -> impl IntoResponse {
    let cb = state.get_circuit_breaker(&service_name);

    match cb {
        Some(circuit_breaker) => {
            circuit_breaker.reset();
            (
                StatusCode::OK,
                Json(ApiResponse::success("Circuit breaker reset successfully")),
            )
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(ApiResponse::<&str>::error("Service not found")),
        ),
    }
}

pub async fn get_global_config(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let config = state.config.read();
    Json(ApiResponse::success(config.clone()))
}

#[derive(Debug, Deserialize)]
pub struct UpdateGlobalConfigRequest {
    pub global_timeout_ms: Option<u64>,
}

pub async fn update_global_config(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<UpdateGlobalConfigRequest>,
) -> impl IntoResponse {
    let mut config = state.config.write();

    if let Some(timeout) = payload.global_timeout_ms {
        config.global_timeout_ms = timeout;
    }

    (
        StatusCode::OK,
        Json(ApiResponse::success(config.clone())),
    )
}
