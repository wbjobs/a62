use rand::Rng;

use crate::config::ServiceConfig;
use crate::types::{ClusterType, RouteTarget, CANARY_HEADER, CANARY_HEADER_VALUE};

#[derive(Clone)]
pub struct TrafficColorer {
    canary_header: String,
    canary_value: String,
}

impl TrafficColorer {
    pub fn new() -> Self {
        Self {
            canary_header: CANARY_HEADER.to_string(),
            canary_value: CANARY_HEADER_VALUE.to_string(),
        }
    }

    pub fn should_route_canary(
        &self,
        headers: &http::HeaderMap,
        service_config: &ServiceConfig,
    ) -> bool {
        if service_config.canary_cluster.is_none() {
            return false;
        }

        if let Some(header_value) = headers.get(&self.canary_header) {
            if let Ok(value) = header_value.to_str() {
                if value.to_lowercase() == self.canary_value.to_lowercase() {
                    let ratio = service_config.canary_ratio.clamp(0.0, 1.0);
                    let mut rng = rand::thread_rng();
                    return rng.gen::<f64>() < ratio;
                }
            }
        }

        false
    }

    pub fn select_endpoint(
        &self,
        service_config: &ServiceConfig,
        cluster_type: ClusterType,
    ) -> Option<String> {
        let endpoints = match cluster_type {
            ClusterType::Stable => &service_config.stable_cluster.endpoints,
            ClusterType::Canary => {
                if let Some(canary) = &service_config.canary_cluster {
                    &canary.endpoints
                } else {
                    &service_config.stable_cluster.endpoints
                }
            }
        };

        if endpoints.is_empty() {
            return None;
        }

        let mut rng = rand::thread_rng();
        let idx = rng.gen_range(0..endpoints.len());
        Some(endpoints[idx].clone())
    }

    pub fn determine_route(
        &self,
        service_name: &str,
        headers: &http::HeaderMap,
        service_config: &ServiceConfig,
    ) -> RouteTarget {
        let cluster_type = if self.should_route_canary(headers, service_config) {
            ClusterType::Canary
        } else {
            ClusterType::Stable
        };

        let endpoint = self
            .select_endpoint(service_config, cluster_type)
            .unwrap_or_else(|| service_config.stable_cluster.endpoints[0].clone());

        RouteTarget {
            service_name: service_name.to_string(),
            cluster_type,
            endpoint,
        }
    }

    pub fn inject_canary_headers(
        &self,
        request_headers: &mut http::HeaderMap,
        cluster_type: ClusterType,
    ) {
        if let Ok(value) = http::HeaderValue::from_str(&cluster_type.to_string()) {
            request_headers.insert("x-canary-cluster", value);
        }
    }
}

impl Default for TrafficColorer {
    fn default() -> Self {
        Self::new()
    }
}

pub fn propagate_canary_headers(
    src: &http::HeaderMap,
    dst: &mut http::HeaderMap,
) {
    if let Some(value) = src.get(CANARY_HEADER) {
        dst.insert(CANARY_HEADER, value.clone());
    }
    if let Some(value) = src.get("x-canary-cluster") {
        dst.insert("x-canary-cluster", value.clone());
    }
    if let Some(value) = src.get("x-request-id") {
        dst.insert("x-request-id", value.clone());
    }
}
