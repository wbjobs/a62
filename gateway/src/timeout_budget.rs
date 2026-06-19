use std::time::{Duration, Instant};

use crate::types::GLOBAL_TIMEOUT_HEADER;

#[derive(Debug, Clone)]
pub struct TimeoutBudget {
    start_time: Instant,
    total_budget: Duration,
}

impl TimeoutBudget {
    pub fn new(total_budget_ms: u64) -> Self {
        Self {
            start_time: Instant::now(),
            total_budget: Duration::from_millis(total_budget_ms),
        }
    }

    pub fn from_header_or_new(
        headers: &http::HeaderMap,
        default_budget_ms: u64,
    ) -> (Self, bool) {
        if let Some(value) = headers.get(GLOBAL_TIMEOUT_HEADER) {
            if let Ok(remaining_str) = value.to_str() {
                if let Ok(remaining_ms) = remaining_str.parse::<u64>() {
                    return (
                        Self {
                            start_time: Instant::now(),
                            total_budget: Duration::from_millis(remaining_ms),
                        },
                        true,
                    );
                }
            }
        }

        (
            Self {
                start_time: Instant::now(),
                total_budget: Duration::from_millis(default_budget_ms),
            },
            false,
        )
    }

    pub fn remaining(&self) -> Duration {
        let elapsed = self.start_time.elapsed();
        self.total_budget.saturating_sub(elapsed)
    }

    pub fn remaining_ms(&self) -> u64 {
        self.remaining().as_millis() as u64
    }

    pub fn is_expired(&self) -> bool {
        self.remaining().is_zero()
    }

    pub fn has_enough_budget(&self, required: Duration) -> bool {
        self.remaining() >= required
    }

    pub fn inject_header(&self, headers: &mut http::HeaderMap) {
        let remaining = self.remaining_ms();
        if let Ok(value) = http::HeaderValue::from_str(&remaining.to_string()) {
            headers.insert(GLOBAL_TIMEOUT_HEADER, value);
        }
    }
}
