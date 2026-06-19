use std::sync::Arc;

use axum::{
    routing::{any, get, post, put},
    Router,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use gateway::config::GatewayConfig;
use gateway::state::AppState;

fn build_proxy_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/{service}/{*path}", any(gateway::proxy::proxy_handler))
        .route("/{service}", any(gateway::proxy::proxy_handler))
        .route("/{service}/", any(gateway::proxy::proxy_handler))
        .with_state(state)
}

fn build_admin_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/api/v1/services", get(gateway::admin_api::list_services))
        .route(
            "/api/v1/services/:name",
            get(gateway::admin_api::get_service_status),
        )
        .route(
            "/api/v1/services/:name/config",
            put(gateway::admin_api::update_service_config),
        )
        .route(
            "/api/v1/services/:name/circuit-breaker/reset",
            post(gateway::admin_api::reset_circuit_breaker),
        )
        .route(
            "/api/v1/config",
            get(gateway::admin_api::get_global_config),
        )
        .route(
            "/api/v1/config",
            put(gateway::admin_api::update_global_config),
        )
        .with_state(state)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "gateway=debug,tower_http=debug,axum=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = GatewayConfig::default();
    let state = AppState::new(config.clone());

    let proxy_state = state.clone();
    let admin_state = state.clone();

    let proxy_router = build_proxy_router(proxy_state);
    let admin_router = build_admin_router(admin_state);

    tracing::info!("Gateway listening on {}", config.listen_addr);
    tracing::info!("Admin API listening on {}", config.admin_addr);

    let proxy_listener = tokio::net::TcpListener::bind(&config.listen_addr).await?;
    let admin_listener = tokio::net::TcpListener::bind(&config.admin_addr).await?;

    let proxy_server = axum::serve(proxy_listener, proxy_router);
    let admin_server = axum::serve(admin_listener, admin_router);

    tokio::try_join!(proxy_server, admin_server)?;

    Ok(())
}
