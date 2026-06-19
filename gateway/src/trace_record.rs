use serde::{Deserialize, Serialize};

use crate::trace_sampler::SamplingReason;
use crate::types::ClusterType;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownstreamCall {
    pub service_name: String,
    pub cluster_type: String,
    pub endpoint: String,
    pub method: String,
    pub path: String,
    pub status_code: u16,
    pub duration_ms: u64,
    pub is_error: bool,
    pub is_timeout: bool,
    pub request_headers: std::collections::HashMap<String, String>,
    pub response_headers: std::collections::HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceRecord {
    pub trace_id: String,
    pub request_id: String,
    pub timestamp_ms: u64,
    pub duration_ms: u64,
    pub client_ip: String,
    pub method: String,
    pub path: String,
    pub status_code: u16,
    pub sampled: bool,
    pub sampling_reason: String,
    pub is_canary: bool,
    pub cluster_type: String,
    pub request_headers: std::collections::HashMap<String, String>,
    pub request_body: Option<String>,
    pub response_headers: std::collections::HashMap<String, String>,
    pub response_body: Option<String>,
    pub downstream_calls: Vec<DownstreamCall>,
    pub error_message: Option<String>,
}

impl TraceRecord {
    pub fn new(trace_id: String, request_id: String) -> Self {
        Self {
            trace_id,
            request_id,
            timestamp_ms: now_ms(),
            duration_ms: 0,
            client_ip: String::new(),
            method: String::new(),
            path: String::new(),
            status_code: 0,
            sampled: false,
            sampling_reason: format!("{:?}", SamplingReason::NotSampled),
            is_canary: false,
            cluster_type: String::new(),
            request_headers: std::collections::HashMap::new(),
            request_body: None,
            response_headers: std::collections::HashMap::new(),
            response_body: None,
            downstream_calls: Vec::new(),
            error_message: None,
        }
    }

    pub fn finish(&mut self) {
        self.duration_ms = now_ms() - self.timestamp_ms;
    }

    pub fn with_headers(&mut self, headers: &http::HeaderMap, is_request: bool) {
        let mut map = std::collections::HashMap::new();
        for (key, value) in headers.iter() {
            if let Ok(v) = value.to_str() {
                let k = key.as_str().to_string();
                if should_capture_header(&k) {
                    map.insert(k, v.to_string());
                }
            }
        }
        if is_request {
            self.request_headers = map;
        } else {
            self.response_headers = map;
        }
    }

    pub fn set_body(&mut self, body: &[u8], is_request: bool, max_size: usize) {
        if body.is_empty() {
            return;
        }
        let clamped = &body[..body.len().min(max_size)];
        let body_str = if is_text_content(is_request, &self.request_headers, &self.response_headers) {
            String::from_utf8_lossy(clamped).to_string()
        } else {
            format!("[binary data, {} bytes]", clamped.len())
        };
        if is_request {
            self.request_body = Some(body_str);
        } else {
            self.response_body = Some(body_str);
        }
    }

    pub fn add_downstream_call(&mut self, call: DownstreamCall) {
        self.downstream_calls.push(call);
    }
}

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn should_capture_header(key: &str) -> bool {
    let k = key.to_lowercase();
    if k.contains("cookie") || k.contains("authorization") || k.contains("token") {
        return false;
    }
    true
}

fn is_text_content(
    is_request: bool,
    req_headers: &std::collections::HashMap<String, String>,
    resp_headers: &std::collections::HashMap<String, String>,
) -> bool {
    let headers = if is_request { req_headers } else { resp_headers };
    if let Some(content_type) = headers.get("content-type") {
        let ct = content_type.to_lowercase();
        return ct.contains("text/")
            || ct.contains("application/json")
            || ct.contains("application/xml")
            || ct.contains("application/x-www-form-urlencoded")
            || ct.contains("application/javascript");
    }
    true
}

pub fn cluster_type_to_string(cluster: Option<ClusterType>) -> String {
    match cluster {
        Some(ClusterType::Stable) => "stable".to_string(),
        Some(ClusterType::Canary) => "canary".to_string(),
        None => String::new(),
    }
}

pub fn sampling_reason_to_string(reason: SamplingReason) -> String {
    format!("{:?}", reason)
}
