use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::config::CircuitBreakerConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

struct Metrics {
    total: AtomicU64,
    errors: AtomicU64,
    slow_calls: AtomicU64,
}

impl Metrics {
    fn new() -> Self {
        Self {
            total: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            slow_calls: AtomicU64::new(0),
        }
    }

    fn reset(&self) {
        self.total.store(0, Ordering::SeqCst);
        self.errors.store(0, Ordering::SeqCst);
        self.slow_calls.store(0, Ordering::SeqCst);
    }

    fn record_success(&self, _duration: Duration) {
        self.total.fetch_add(1, Ordering::SeqCst);
    }

    fn record_error(&self, _duration: Duration) {
        self.total.fetch_add(1, Ordering::SeqCst);
        self.errors.fetch_add(1, Ordering::SeqCst);
    }

    fn record_slow_call(&self, is_error: bool) {
        self.total.fetch_add(1, Ordering::SeqCst);
        self.slow_calls.fetch_add(1, Ordering::SeqCst);
        if is_error {
            self.errors.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn total(&self) -> u64 {
        self.total.load(Ordering::SeqCst)
    }

    fn error_count(&self) -> u64 {
        self.errors.load(Ordering::SeqCst)
    }

    fn slow_call_count(&self) -> u64 {
        self.slow_calls.load(Ordering::SeqCst)
    }
}

struct Window {
    metrics: Metrics,
}

impl Window {
    fn new() -> Self {
        Self {
            metrics: Metrics::new(),
        }
    }

    fn reset(&self) {
        self.metrics.reset();
    }
}

pub struct CircuitBreaker {
    config: Mutex<CircuitBreakerConfig>,
    state: Mutex<CircuitState>,
    window: Window,
    open_time: Mutex<Option<Instant>>,
    half_open_attempts: AtomicU64,
    half_open_success: AtomicU64,
}

impl CircuitBreaker {
    pub fn new(config: CircuitBreakerConfig) -> Arc<Self> {
        Arc::new(Self {
            config: Mutex::new(config),
            state: Mutex::new(CircuitState::Closed),
            window: Window::new(),
            open_time: Mutex::new(None),
            half_open_attempts: AtomicU64::new(0),
            half_open_success: AtomicU64::new(0),
        })
    }

    pub fn update_config(&self, new_config: CircuitBreakerConfig) {
        let mut config = self.config.lock();
        *config = new_config;
    }

    pub fn state(&self) -> CircuitState {
        self.try_transition_state();
        *self.state.lock()
    }

    pub fn allow_request(&self) -> bool {
        self.try_transition_state();

        let state = *self.state.lock();
        match state {
            CircuitState::Closed => true,
            CircuitState::Open => false,
            CircuitState::HalfOpen => {
                let attempts = self.half_open_attempts.fetch_add(1, Ordering::SeqCst);
                attempts < 5
            }
        }
    }

    fn try_transition_state(&self) {
        let config = self.config.lock().clone();
        let mut state = self.state.lock();
        let mut open_time = self.open_time.lock();

        match *state {
            CircuitState::Closed => {
                if self.should_open(&config) {
                    *state = CircuitState::Open;
                    *open_time = Some(Instant::now());
                }
            }
            CircuitState::Open => {
                if let Some(opened_at) = *open_time {
                    if opened_at.elapsed() >= config.sleep_window() {
                        *state = CircuitState::HalfOpen;
                        self.half_open_attempts.store(0, Ordering::SeqCst);
                        self.half_open_success.store(0, Ordering::SeqCst);
                        self.window.reset();
                    }
                }
            }
            CircuitState::HalfOpen => {
                let attempts = self.half_open_attempts.load(Ordering::SeqCst);
                let success = self.half_open_success.load(Ordering::SeqCst);

                if attempts >= 5 {
                    if success >= 4 {
                        *state = CircuitState::Closed;
                        *open_time = None;
                        self.window.reset();
                    } else {
                        *state = CircuitState::Open;
                        *open_time = Some(Instant::now());
                    }
                }
            }
        }
    }

    fn should_open(&self, config: &CircuitBreakerConfig) -> bool {
        let total = self.window.metrics.total();
        if total < config.request_volume_threshold {
            return false;
        }

        let error_count = self.window.metrics.error_count();
        let error_ratio = error_count as f64 / total as f64;
        if error_ratio >= config.error_threshold {
            return true;
        }

        let slow_count = self.window.metrics.slow_call_count();
        let slow_ratio = slow_count as f64 / total as f64;
        if slow_ratio >= config.slow_call_ratio_threshold {
            return true;
        }

        false
    }

    pub fn record_success(&self, duration: Duration) {
        let state = *self.state.lock();

        match state {
            CircuitState::Closed => {
                let config = self.config.lock().clone();
                if duration >= config.slow_call_threshold() {
                    self.window.metrics.record_slow_call(false);
                } else {
                    self.window.metrics.record_success(duration);
                }
            }
            CircuitState::HalfOpen => {
                self.half_open_success.fetch_add(1, Ordering::SeqCst);
            }
            CircuitState::Open => {}
        }
    }

    pub fn record_error(&self, duration: Duration) {
        let state = *self.state.lock();

        match state {
            CircuitState::Closed => {
                let config = self.config.lock().clone();
                if duration >= config.slow_call_threshold() {
                    self.window.metrics.record_slow_call(true);
                } else {
                    self.window.metrics.record_error(duration);
                }
            }
            CircuitState::HalfOpen => {}
            CircuitState::Open => {}
        }
    }

    pub fn metrics(&self) -> (u64, u64, u64) {
        (
            self.window.metrics.total(),
            self.window.metrics.error_count(),
            self.window.metrics.slow_call_count(),
        )
    }

    pub fn reset(&self) {
        let mut state = self.state.lock();
        *state = CircuitState::Closed;
        let mut open_time = self.open_time.lock();
        *open_time = None;
        self.window.reset();
        self.half_open_attempts.store(0, Ordering::SeqCst);
        self.half_open_success.store(0, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn test_config() -> CircuitBreakerConfig {
        CircuitBreakerConfig {
            error_threshold: 0.5,
            request_volume_threshold: 10,
            sleep_window_ms: 100,
            slow_call_threshold_ms: 500,
            slow_call_ratio_threshold: 0.8,
        }
    }

    #[test]
    fn test_circuit_breaker_starts_closed() {
        let cb = CircuitBreaker::new(test_config());
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.allow_request());
    }

    #[test]
    fn test_circuit_breaker_opens_on_high_error_rate() {
        let cb = CircuitBreaker::new(test_config());

        for _ in 0..15 {
            cb.record_error(Duration::from_millis(10));
        }

        assert_eq!(cb.state(), CircuitState::Open);
        assert!(!cb.allow_request());
    }

    #[test]
    fn test_circuit_breaker_stays_closed_below_threshold() {
        let cb = CircuitBreaker::new(test_config());

        for _ in 0..5 {
            cb.record_error(Duration::from_millis(10));
        }
        for _ in 0..10 {
            cb.record_success(Duration::from_millis(10));
        }

        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.allow_request());
    }

    #[test]
    fn test_circuit_breaker_half_open_after_sleep_window() {
        let config = CircuitBreakerConfig {
            sleep_window_ms: 50,
            ..test_config()
        };
        let cb = CircuitBreaker::new(config);

        for _ in 0..15 {
            cb.record_error(Duration::from_millis(10));
        }
        assert_eq!(cb.state(), CircuitState::Open);

        std::thread::sleep(Duration::from_millis(60));

        assert_eq!(cb.state(), CircuitState::HalfOpen);
        assert!(cb.allow_request());
    }

    #[test]
    fn test_metrics_tracking() {
        let cb = CircuitBreaker::new(test_config());

        for _ in 0..7 {
            cb.record_success(Duration::from_millis(10));
        }
        for _ in 0..3 {
            cb.record_error(Duration::from_millis(10));
        }

        let (total, errors, slow) = cb.metrics();
        assert_eq!(total, 10);
        assert_eq!(errors, 3);
        assert_eq!(slow, 0);
    }

    #[test]
    fn test_slow_call_detection() {
        let config = CircuitBreakerConfig {
            slow_call_threshold_ms: 100,
            slow_call_ratio_threshold: 0.5,
            request_volume_threshold: 5,
            ..test_config()
        };
        let cb = CircuitBreaker::new(config);

        for _ in 0..6 {
            cb.record_success(Duration::from_millis(200));
        }

        let (total, _errors, slow) = cb.metrics();
        assert_eq!(total, 6);
        assert_eq!(slow, 6);
    }

    #[test]
    fn test_reset() {
        let cb = CircuitBreaker::new(test_config());

        for _ in 0..15 {
            cb.record_error(Duration::from_millis(10));
        }
        assert_eq!(cb.state(), CircuitState::Open);

        cb.reset();
        assert_eq!(cb.state(), CircuitState::Closed);
        let (total, errors, _) = cb.metrics();
        assert_eq!(total, 0);
        assert_eq!(errors, 0);
    }
}
