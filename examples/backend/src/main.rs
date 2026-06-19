use std::env;

use axum::{
    extract::State,
    http::HeaderMap,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
struct AppState {
    service_name: String,
    cluster_type: String,
    port: u16,
}

#[derive(Debug, Serialize, Deserialize)]
struct EchoResponse {
    service: String,
    cluster: String,
    path: String,
    headers: std::collections::HashMap<String, String>,
}

async fn echo_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(path): axum::extract::Path<String>,
) -> Json<EchoResponse> {
    let mut header_map = std::collections::HashMap::new();
    for (key, value) in headers.iter() {
        if let Ok(val) = value.to_str() {
            header_map.insert(key.as_str().to_string(), val.to_string());
        }
    }

    Json(EchoResponse {
        service: state.service_name.clone(),
        cluster: state.cluster_type.clone(),
        path,
        headers: header_map,
    })
}

async fn health_handler() -> &'static str {
    "OK"
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let args: Vec<String> = env::args().collect();

    let service_name = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "user-service".to_string());
    let cluster_type = args
        .get(2)
        .cloned()
        .unwrap_or_else(|| "stable".to_string());
    let port: u16 = args
        .get(3)
        .and_then(|p| p.parse().ok())
        .unwrap_or(8081);

    let state = AppState {
        service_name: service_name.clone(),
        cluster_type: cluster_type.clone(),
        port,
    };

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/*path", get(echo_handler))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    tracing::info!(
        "Backend service {} ({}) listening on {}",
        service_name,
        cluster_type,
        addr
    );

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
