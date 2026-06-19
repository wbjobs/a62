use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::RwLock;
use reqwest::Client;

use crate::config::{GatewayConfig, ServiceConfig};
use crate::circuit_breaker::CircuitBreaker;
use crate::traffic_coloring::TrafficColorer;

pub struct AppState {
    pub config: RwLock<GatewayConfig>,
    pub circuit_breakers: DashMap<String, Arc<CircuitBreaker>>,
    pub traffic_colorer: TrafficColorer,
    pub http_client: Client,
}

impl AppState {
    pub fn new(config: GatewayConfig) -> Arc<Self> {
        let circuit_breakers = DashMap::new();

        for (name, service) in &config.services {
            let cb = CircuitBreaker::new(service.circuit_breaker.clone());
            circuit_breakers.insert(name.clone(), cb);
        }

        let http_client = Client::builder()
            .pool_max_idle_per_host(32)
            .http2_keep_alive_timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");

        Arc::new(Self {
            config: RwLock::new(config),
            circuit_breakers,
            traffic_colorer: TrafficColorer::new(),
            http_client,
        })
    }

    pub fn update_service_config(&self, service_name: &str, service_config: ServiceConfig) {
        let mut config = self.config.write();
        config
            .services
            .insert(service_name.to_string(), service_config.clone());

        if let Some(cb_entry) = self.circuit_breakers.get(service_name) {
            cb_entry.update_config(service_config.circuit_breaker.clone());
        } else {
            let cb = CircuitBreaker::new(service_config.circuit_breaker);
            self.circuit_breakers.insert(service_name.to_string(), cb);
        }
    }

    pub fn get_service(&self, name: &str) -> Option<ServiceConfig> {
        let config = self.config.read();
        config.services.get(name).cloned()
    }

    pub fn get_circuit_breaker(&self, service_name: &str) -> Option<Arc<CircuitBreaker>> {
        self.circuit_breakers.get(service_name).map(|cb| cb.clone())
    }

    pub fn global_timeout_ms(&self) -> u64 {
        let config = self.config.read();
        config.global_timeout_ms
    }
}
