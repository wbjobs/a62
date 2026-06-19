use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

use crate::config::CircuitBreakerConfig;

const STATE_CLOSED: usize = 0;
const STATE_OPEN: usize = 1;
const STATE_HALF_OPEN: usize = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

impl From<usize> for CircuitState {
    fn from(value: usize) -> Self {
        match value {
            STATE_CLOSED => CircuitState::Closed,
            STATE_OPEN => CircuitState::Open,
            STATE_HALF_OPEN => CircuitState::HalfOpen,
            _ => CircuitState::Closed,
        }
    }
}

impl From<CircuitState> for usize {
    fn from(state: CircuitState) -> Self {
        match state {
            CircuitState::Closed => STATE_CLOSED,
            CircuitState::Open => STATE_OPEN,
            CircuitState::HalfOpen => STATE_HALF_OPEN,
        }
    }
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
        self.total.store(0, Ordering::Relaxed);
        self.errors.store(0, Ordering::Relaxed);
        self.slow_calls.store(0, Ordering::Relaxed);
    }

    fn record_success(&self) {
        self.total.fetch_add(1, Ordering::Relaxed);
    }

    fn record_error(&self) {
        self.total.fetch_add(1, Ordering::Relaxed);
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    fn record_slow_call(&self, is_error: bool) {
        self.total.fetch_add(1, Ordering::Relaxed);
        self.slow_calls.fetch_add(1, Ordering::Relaxed);
        if is_error {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn total(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }

    fn error_count(&self) -> u64 {
        self.errors.load(Ordering::Relaxed)
    }

    fn slow_call_count(&self) -> u64 {
        self.slow_calls.load(Ordering::Relaxed)
    }
}

struct ConfigSnapshot {
    error_threshold: u64,
    request_volume_threshold: u64,
    sleep_window_ms: u64,
    slow_call_threshold_ms: u64,
    slow_call_ratio_threshold: u64,
}

impl ConfigSnapshot {
    fn from_config(config: &CircuitBreakerConfig) -> Self {
        Self {
            error_threshold: (config.error_threshold * 1_000_000.0) as u64,
            request_volume_threshold: config.request_volume_threshold,
            sleep_window_ms: config.sleep_window_ms,
            slow_call_threshold_ms: config.slow_call_threshold_ms,
            slow_call_ratio_threshold: (config.slow_call_ratio_threshold * 1_000_000.0) as u64,
        }
    }
}

pub struct CircuitBreaker {
    state: AtomicUsize,
    open_time: RwLock<Option<Instant>>,
    half_open_attempts: AtomicU64,
    half_open_success: AtomicU64,
    window: Metrics,
    config: std::sync::RwLock<CircuitBreakerConfig>,
    config_snapshot: std::sync::RwLock<ConfigSnapshot>,
}

impl CircuitBreaker {
    pub fn new(config: CircuitBreakerConfig) -> Arc<Self> {
        let snapshot = ConfigSnapshot::from_config(&config);
        Arc::new(Self {
            state: AtomicUsize::new(STATE_CLOSED),
            open_time: RwLock::new(None),
            half_open_attempts: AtomicU64::new(0),
            half_open_success: AtomicU64::new(0),
            window: Metrics::new(),
            config: std::sync::RwLock::new(config),
            config_snapshot: std::sync::RwLock::new(snapshot),
        })
    }

    pub fn update_config(&self, new_config: CircuitBreakerConfig) {
        let snapshot = ConfigSnapshot::from_config(&new_config);
        *self.config.write().unwrap() = new_config;
        *self.config_snapshot.write().unwrap() = snapshot;
    }

    fn load_config(&self) -> ConfigSnapshot {
        let guard = self.config_snapshot.read().unwrap();
        ConfigSnapshot {
            error_threshold: guard.error_threshold,
            request_volume_threshold: guard.request_volume_threshold,
            sleep_window_ms: guard.sleep_window_ms,
            slow_call_threshold_ms: guard.slow_call_threshold_ms,
            slow_call_ratio_threshold: guard.slow_call_ratio_threshold,
        }
    }

    fn current_state(&self) -> CircuitState {
        self.state.load(Ordering::Acquire).into()
    }

    fn cas_state(&self, current: CircuitState, new: CircuitState) -> bool {
        let current_usize: usize = current.into();
        let new_usize: usize = new.into();
        self.state
            .compare_exchange(current_usize, new_usize, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn set_open_time(&self, time: Option<Instant>) {
        let mut guard = self.open_time.write();
        *guard = time;
    }

    fn get_open_time(&self) -> Option<Instant> {
        *self.open_time.read()
    }

    pub fn state(&self) -> CircuitState {
        self.try_transition_state();
        self.current_state()
    }

    pub fn allow_request(&self) -> bool {
        self.try_transition_state();

        match self.current_state() {
            CircuitState::Closed => true,
            CircuitState::Open => false,
            CircuitState::HalfOpen => {
                let attempts = self.half_open_attempts.fetch_add(1, Ordering::Relaxed);
                attempts < 5
            }
        }
    }

    fn try_transition_state(&self) {
        let config = self.load_config();
        let current_state = self.current_state();

        match current_state {
            CircuitState::Closed => {
                if self.should_open(&config) {
                    if self.cas_state(CircuitState::Closed, CircuitState::Open) {
                        self.set_open_time(Some(Instant::now()));
                    }
                }
            }
            CircuitState::Open => {
                if let Some(opened_at) = self.get_open_time() {
                    if opened_at.elapsed() >= Duration::from_millis(config.sleep_window_ms) {
                        if self.cas_state(CircuitState::Open, CircuitState::HalfOpen) {
                            self.half_open_attempts.store(0, Ordering::Relaxed);
                            self.half_open_success.store(0, Ordering::Relaxed);
                            self.window.reset();
                        }
                    }
                }
            }
            CircuitState::HalfOpen => {
                let attempts = self.half_open_attempts.load(Ordering::Relaxed);
                let success = self.half_open_success.load(Ordering::Relaxed);

                if attempts >= 5 {
                    if success >= 4 {
                        if self.cas_state(CircuitState::HalfOpen, CircuitState::Closed) {
                            self.set_open_time(None);
                            self.window.reset();
                        }
                    } else {
                        if self.cas_state(CircuitState::HalfOpen, CircuitState::Open) {
                            self.set_open_time(Some(Instant::now()));
                        }
                    }
                }
            }
        }
    }

    fn should_open(&self, config: &ConfigSnapshot) -> bool {
        let total = self.window.total();
        if total < config.request_volume_threshold {
            return false;
        }

        let error_count = self.window.error_count();
        let error_ratio_scaled = (error_count as u128 * 1_000_000) / total as u128;
        if error_ratio_scaled >= config.error_threshold as u128 {
            return true;
        }

        let slow_count = self.window.slow_call_count();
        let slow_ratio_scaled = (slow_count as u128 * 1_000_000) / total as u128;
        if slow_ratio_scaled >= config.slow_call_ratio_threshold as u128 {
            return true;
        }

        false
    }

    pub fn record_success(&self, duration: Duration) {
        let state = self.current_state();

        match state {
            CircuitState::Closed => {
                let config = self.load_config();
                if duration.as_millis() >= config.slow_call_threshold_ms as u128 {
                    self.window.record_slow_call(false);
                } else {
                    self.window.record_success();
                }
            }
            CircuitState::HalfOpen => {
                self.half_open_success.fetch_add(1, Ordering::Relaxed);
            }
            CircuitState::Open => {}
        }
    }

    pub fn record_error(&self, duration: Duration) {
        let state = self.current_state();

        match state {
            CircuitState::Closed => {
                let config = self.load_config();
                if duration.as_millis() >= config.slow_call_threshold_ms as u128 {
                    self.window.record_slow_call(true);
                } else {
                    self.window.record_error();
                }
            }
            CircuitState::HalfOpen => {}
            CircuitState::Open => {}
        }
    }

    pub fn metrics(&self) -> (u64, u64, u64) {
        (
            self.window.total(),
            self.window.error_count(),
            self.window.slow_call_count(),
        )
    }

    pub fn reset(&self) {
        self.state.store(STATE_CLOSED, Ordering::Release);
        self.set_open_time(None);
        self.window.reset();
        self.half_open_attempts.store(0, Ordering::Relaxed);
        self.half_open_success.store(0, Ordering::Relaxed);
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_access_no_deadlock() {
        use std::sync::Arc;

        let cb = CircuitBreaker::new(test_config());
        let mut handles = vec![];

        for i in 0..100 {
            let cb_clone = cb.clone();
            handles.push(tokio::spawn(async move {
                for j in 0..100 {
                    if cb_clone.allow_request() {
                        if (i + j) % 3 == 0 {
                            cb_clone.record_error(Duration::from_millis(10));
                        } else {
                            cb_clone.record_success(Duration::from_millis(10));
                        }
                    }
                    tokio::task::yield_now().await;
                }
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let (total, errors, _) = cb.metrics();
        assert!(total > 0);
        assert!(errors > 0);
    }
}
