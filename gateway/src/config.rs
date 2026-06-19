use serde::{Deserialize, Serialize};
use std::time::Duration;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayConfig {
    pub listen_addr: String,
    pub admin_addr: String,
    pub global_timeout_ms: u64,
    pub services: HashMap<String, ServiceConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceConfig {
    pub name: String,
    pub stable_cluster: ClusterConfig,
    pub canary_cluster: Option<ClusterConfig>,
    pub canary_ratio: f64,
    pub circuit_breaker: CircuitBreakerConfig,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub endpoints: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    pub error_threshold: f64,
    pub request_volume_threshold: u64,
    pub sleep_window_ms: u64,
    pub slow_call_threshold_ms: u64,
    pub slow_call_ratio_threshold: f64,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        let mut services = HashMap::new();
        services.insert(
            "user-service".to_string(),
            ServiceConfig {
                name: "user-service".to_string(),
                stable_cluster: ClusterConfig {
                    endpoints: vec!["http://127.0.0.1:8081".to_string()],
                },
                canary_cluster: Some(ClusterConfig {
                    endpoints: vec!["http://127.0.0.1:8082".to_string()],
                }),
                canary_ratio: 0.1,
                circuit_breaker: CircuitBreakerConfig {
                    error_threshold: 0.5,
                    request_volume_threshold: 20,
                    sleep_window_ms: 5000,
                    slow_call_threshold_ms: 1000,
                    slow_call_ratio_threshold: 0.8,
                },
                timeout_ms: 3000,
            },
        );

        services.insert(
            "order-service".to_string(),
            ServiceConfig {
                name: "order-service".to_string(),
                stable_cluster: ClusterConfig {
                    endpoints: vec!["http://127.0.0.1:8091".to_string()],
                },
                canary_cluster: Some(ClusterConfig {
                    endpoints: vec!["http://127.0.0.1:8092".to_string()],
                }),
                canary_ratio: 0.2,
                circuit_breaker: CircuitBreakerConfig {
                    error_threshold: 0.5,
                    request_volume_threshold: 20,
                    sleep_window_ms: 5000,
                    slow_call_threshold_ms: 1000,
                    slow_call_ratio_threshold: 0.8,
                },
                timeout_ms: 3000,
            },
        );

        Self {
            listen_addr: "0.0.0.0:8080".to_string(),
            admin_addr: "0.0.0.0:9090".to_string(),
            global_timeout_ms: 5000,
            services,
        }
    }
}

impl CircuitBreakerConfig {
    pub fn sleep_window(&self) -> Duration {
        Duration::from_millis(self.sleep_window_ms)
    }

    pub fn slow_call_threshold(&self) -> Duration {
        Duration::from_millis(self.slow_call_threshold_ms)
    }
}

impl ServiceConfig {
    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }
}
