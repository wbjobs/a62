use std::time::{Duration, Instant};

use crate::timeout_budget::TimeoutBudget;
use crate::trace_sampler::{SamplingReason, TRACE_ID_HEADER, SAMPLED_HEADER};
use crate::types::{ClusterType, CANARY_HEADER, CANARY_CLUSTER_HEADER};

#[derive(Debug, Clone)]
pub struct SpanContext {
    pub trace_id: String,
    pub request_id: String,
    pub timeout_budget: TimeoutBudget,
    pub is_canary: bool,
    pub cluster_type: Option<ClusterType>,
    pub sampled: bool,
    pub sampling_reason: SamplingReason,
    pub created_at: Instant,
}

impl SpanContext {
    pub fn new(request_id: String, total_budget_ms: u64) -> Self {
        Self {
            trace_id: generate_trace_id(),
            request_id,
            timeout_budget: TimeoutBudget::new(total_budget_ms),
            is_canary: false,
            cluster_type: None,
            sampled: false,
            sampling_reason: SamplingReason::NotSampled,
            created_at: Instant::now(),
        }
    }

    pub fn from_headers(headers: &http::HeaderMap, default_budget_ms: u64) -> Self {
        let trace_id = headers
            .get(TRACE_ID_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .unwrap_or_else(|| generate_trace_id());

        let request_id = headers
            .get("x-request-id")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .unwrap_or_else(|| generate_request_id());

        let (timeout_budget, _from_header) =
            TimeoutBudget::from_header_or_new(headers, default_budget_ms);

        let is_canary = headers
            .get(CANARY_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.to_lowercase() == "canary")
            .unwrap_or(false);

        let cluster_type = headers
            .get(CANARY_CLUSTER_HEADER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| match v.to_lowercase().as_str() {
                "stable" => Some(ClusterType::Stable),
                "canary" => Some(ClusterType::Canary),
                _ => None,
            });

        let sampled = headers
            .get(SAMPLED_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        let sampling_reason = if sampled {
            SamplingReason::HeaderForced
        } else {
            SamplingReason::NotSampled
        };

        Self {
            trace_id,
            request_id,
            timeout_budget,
            is_canary,
            cluster_type,
            sampled,
            sampling_reason,
            created_at: Instant::now(),
        }
    }

    pub fn with_sampling(mut self, sampled: bool, reason: SamplingReason, trace_id: String) -> Self {
        self.sampled = sampled;
        self.sampling_reason = reason;
        self.trace_id = trace_id;
        self
    }

    pub fn remaining_budget(&self) -> Duration {
        self.timeout_budget.remaining()
    }

    pub fn remaining_budget_ms(&self) -> u64 {
        self.timeout_budget.remaining_ms()
    }

    pub fn is_budget_expired(&self) -> bool {
        self.timeout_budget.is_expired()
    }

    pub fn has_enough_budget(&self, required: Duration) -> bool {
        self.timeout_budget.has_enough_budget(required)
    }

    pub fn set_cluster_type(&mut self, cluster: ClusterType) {
        self.cluster_type = Some(cluster);
    }

    pub fn set_sampled(&mut self, sampled: bool, reason: SamplingReason) {
        self.sampled = sampled;
        self.sampling_reason = reason;
    }

    pub fn inject_headers(&self, headers: &mut http::HeaderMap) {
        if let Ok(value) = http::HeaderValue::from_str(&self.trace_id) {
            headers.insert(TRACE_ID_HEADER, value);
        }

        if let Ok(value) = http::HeaderValue::from_str(&self.request_id) {
            headers.insert("x-request-id", value);
        }

        if self.is_canary {
            headers.insert(CANARY_HEADER, http::HeaderValue::from_static("canary"));
        }

        if let Some(cluster) = self.cluster_type {
            if let Ok(value) = http::HeaderValue::from_str(&cluster.to_string()) {
                headers.insert(CANARY_CLUSTER_HEADER, value);
            }
        }

        let sampled_value = if self.sampled { "1" } else { "0" };
        headers.insert(SAMPLED_HEADER, http::HeaderValue::from_static(sampled_value));

        self.timeout_budget.inject_header(headers);
    }

    pub fn with_request_id(mut self, request_id: String) -> Self {
        self.request_id = request_id;
        self
    }

    pub fn with_canary(mut self, is_canary: bool) -> Self {
        self.is_canary = is_canary;
        self
    }
}

fn generate_request_id() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    hex_encode(&bytes)
}

fn generate_trace_id() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    hex_encode(&bytes)
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

tokio::task_local! {
    pub static CURRENT_SPAN: std::cell::RefCell<Option<SpanContext>>;
}

pub async fn with_span_context<F, Fut, T>(ctx: SpanContext, f: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    CURRENT_SPAN
        .scope(std::cell::RefCell::new(Some(ctx)), f())
        .await
}

pub fn try_get_span_context() -> Option<SpanContext> {
    match CURRENT_SPAN.try_with(|cell| cell.borrow().clone()) {
        Ok(Some(ctx)) => Some(ctx),
        _ => None,
    }
}

pub fn get_span_context() -> SpanContext {
    try_get_span_context().expect("SpanContext not set in current task")
}

pub fn update_span_context<F>(f: F)
where
    F: FnOnce(&mut SpanContext),
{
    let _ = CURRENT_SPAN.try_with(|cell| {
        if let Some(ref mut ctx) = *cell.borrow_mut() {
            f(ctx);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_span_context_basic() {
        let ctx = SpanContext::new("test-id".to_string(), 5000);
        assert_eq!(ctx.request_id, "test-id");
        assert_eq!(ctx.remaining_budget_ms(), 5000);
        assert!(!ctx.is_budget_expired());
    }

    #[tokio::test]
    async fn test_span_context_from_headers() {
        let mut headers = http::HeaderMap::new();
        headers.insert(CANARY_HEADER, http::HeaderValue::from_static("canary"));
        headers.insert(
            CANARY_CLUSTER_HEADER,
            http::HeaderValue::from_static("canary"),
        );
        headers.insert(
            GLOBAL_TIMEOUT_HEADER,
            http::HeaderValue::from_static("3000"),
        );

        let ctx = SpanContext::from_headers(&headers, 5000);
        assert!(ctx.is_canary);
        assert_eq!(ctx.cluster_type, Some(ClusterType::Canary));
        assert!(ctx.remaining_budget_ms() <= 3000);
    }

    #[tokio::test]
    async fn test_span_context_inject_headers() {
        let mut ctx = SpanContext::new("test-id".to_string(), 5000);
        ctx.is_canary = true;
        ctx.cluster_type = Some(ClusterType::Canary);
        ctx.sampled = true;
        ctx.sampling_reason = SamplingReason::CanaryForced;
        ctx.trace_id = "trace-123".to_string();

        let mut headers = http::HeaderMap::new();
        ctx.inject_headers(&mut headers);

        assert_eq!(
            headers.get("x-trace-id").and_then(|v| v.to_str().ok()),
            Some("trace-123")
        );
        assert_eq!(
            headers.get("x-request-id").and_then(|v| v.to_str().ok()),
            Some("test-id")
        );
        assert_eq!(
            headers.get(CANARY_HEADER).and_then(|v| v.to_str().ok()),
            Some("canary")
        );
        assert_eq!(
            headers.get(CANARY_CLUSTER_HEADER).and_then(|v| v.to_str().ok()),
            Some("canary")
        );
        assert_eq!(
            headers.get("x-trace-sampled").and_then(|v| v.to_str().ok()),
            Some("1")
        );
    }

    #[tokio::test]
    async fn test_task_local_span_context() {
        let ctx = SpanContext::new("test-123".to_string(), 5000);

        let result = with_span_context(ctx, || async {
            let got = get_span_context();
            assert_eq!(got.request_id, "test-123");

            tokio::task::yield_now().await;

            let got2 = get_span_context();
            assert_eq!(got2.request_id, "test-123");
            assert!(!got2.is_budget_expired());

            "success"
        })
        .await;

        assert_eq!(result, "success");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_task_local_across_await_points() {
        let ctx = SpanContext::new("persistent-id".to_string(), 5000);

        with_span_context(ctx, || async move {
            for i in 0..5 {
                tokio::task::yield_now().await;
                let span = get_span_context();
                assert_eq!(span.request_id, "persistent-id");
                assert!(!span.is_budget_expired());

                if i == 2 {
                    update_span_context(|ctx| {
                        ctx.is_canary = true;
                        ctx.cluster_type = Some(ClusterType::Canary);
                    });
                }

                if i > 2 {
                    let span = get_span_context();
                    assert!(span.is_canary);
                    assert_eq!(span.cluster_type, Some(ClusterType::Canary));
                }
            }
        })
        .await;
    }

    #[tokio::test]
    async fn test_nested_span_context() {
        let outer = SpanContext::new("outer".to_string(), 5000);

        with_span_context(outer, || async {
            assert_eq!(get_span_context().request_id, "outer");

            let inner = SpanContext::new("inner".to_string(), 3000);
            with_span_context(inner, || async {
                assert_eq!(get_span_context().request_id, "inner");
                tokio::task::yield_now().await;
                assert_eq!(get_span_context().request_id, "inner");
            })
            .await;

            assert_eq!(get_span_context().request_id, "outer");
        })
        .await;
    }
}
