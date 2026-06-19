use std::collections::HashMap;

use crate::types::{CANARY_HEADER, CANARY_CLUSTER_HEADER, GLOBAL_TIMEOUT_HEADER};

pub const CANARY_METADATA_KEY: &str = "x-envoy";
pub const CANARY_CLUSTER_METADATA_KEY: &str = "x-canary-cluster";
pub const GLOBAL_TIMEOUT_METADATA_KEY: &str = "x-global-timeout-remaining-ms";

pub fn http_headers_to_grpc_metadata(headers: &http::HeaderMap) -> HashMap<String, String> {
    let mut metadata = HashMap::new();

    if let Some(value) = headers.get(CANARY_HEADER) {
        if let Ok(v) = value.to_str() {
            metadata.insert(CANARY_METADATA_KEY.to_string(), v.to_string());
        }
    }

    if let Some(value) = headers.get(CANARY_CLUSTER_HEADER) {
        if let Ok(v) = value.to_str() {
            metadata.insert(CANARY_CLUSTER_METADATA_KEY.to_string(), v.to_string());
        }
    }

    if let Some(value) = headers.get(GLOBAL_TIMEOUT_HEADER) {
        if let Ok(v) = value.to_str() {
            metadata.insert(GLOBAL_TIMEOUT_METADATA_KEY.to_string(), v.to_string());
        }
    }

    if let Some(value) = headers.get("x-request-id") {
        if let Ok(v) = value.to_str() {
            metadata.insert("x-request-id".to_string(), v.to_string());
        }
    }

    metadata
}

pub fn grpc_metadata_to_http_headers(metadata: &HashMap<String, String>) -> http::HeaderMap {
    let mut headers = http::HeaderMap::new();

    if let Some(value) = metadata.get(CANARY_METADATA_KEY) {
        if let Ok(v) = http::HeaderValue::from_str(value) {
            headers.insert(CANARY_HEADER, v);
        }
    }

    if let Some(value) = metadata.get(CANARY_CLUSTER_METADATA_KEY) {
        if let Ok(v) = http::HeaderValue::from_str(value) {
            headers.insert(CANARY_CLUSTER_HEADER, v);
        }
    }

    if let Some(value) = metadata.get(GLOBAL_TIMEOUT_METADATA_KEY) {
        if let Ok(v) = http::HeaderValue::from_str(value) {
            headers.insert(GLOBAL_TIMEOUT_HEADER, v);
        }
    }

    if let Some(value) = metadata.get("x-request-id") {
        if let Ok(v) = http::HeaderValue::from_str(value) {
            headers.insert("x-request-id", v);
        }
    }

    headers
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_to_grpc_metadata() {
        let mut headers = http::HeaderMap::new();
        headers.insert(CANARY_HEADER, http::HeaderValue::from_static("canary"));
        headers.insert(
            CANARY_CLUSTER_HEADER,
            http::HeaderValue::from_static("canary"),
        );
        headers.insert(
            GLOBAL_TIMEOUT_HEADER,
            http::HeaderValue::from_static("5000"),
        );

        let metadata = http_headers_to_grpc_metadata(&headers);
        assert_eq!(metadata.get(CANARY_METADATA_KEY), Some(&"canary".to_string()));
        assert_eq!(
            metadata.get(CANARY_CLUSTER_METADATA_KEY),
            Some(&"canary".to_string())
        );
        assert_eq!(
            metadata.get(GLOBAL_TIMEOUT_METADATA_KEY),
            Some(&"5000".to_string())
        );
    }

    #[test]
    fn test_grpc_to_http_headers() {
        let mut metadata = HashMap::new();
        metadata.insert(CANARY_METADATA_KEY.to_string(), "canary".to_string());
        metadata.insert(
            CANARY_CLUSTER_METADATA_KEY.to_string(),
            "canary".to_string(),
        );
        metadata.insert(
            GLOBAL_TIMEOUT_METADATA_KEY.to_string(),
            "5000".to_string(),
        );

        let headers = grpc_metadata_to_http_headers(&metadata);
        assert_eq!(
            headers.get(CANARY_HEADER).and_then(|v| v.to_str().ok()),
            Some("canary")
        );
        assert_eq!(
            headers.get(CANARY_CLUSTER_HEADER).and_then(|v| v.to_str().ok()),
            Some("canary")
        );
        assert_eq!(
            headers.get(GLOBAL_TIMEOUT_HEADER).and_then(|v| v.to_str().ok()),
            Some("5000")
        );
    }
}
