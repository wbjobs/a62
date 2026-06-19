use std::sync::Arc;

use rand::Rng;
use regex::Regex;
use twox_hash::XxHash64;
use std::hash::{Hash, Hasher};

use crate::types::CANARY_HEADER;

pub const SAMPLED_HEADER: &str = "x-trace-sampled";
pub const TRACE_ID_HEADER: &str = "x-trace-id";

pub struct SamplingDecision {
    pub sampled: bool,
    pub reason: SamplingReason,
    pub trace_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SamplingReason {
    NotSampled,
    Percentage,
    CanaryForced,
    UrlRegex,
    HeaderForced,
}

pub struct SamplerConfig {
    pub percentage: f64,
    pub force_canary: bool,
    pub url_patterns: Vec<String>,
}

impl Default for SamplerConfig {
    fn default() -> Self {
        Self {
            percentage: 0.01,
            force_canary: true,
            url_patterns: vec![r"^/api/v1/orders/.*".to_string()],
        }
    }
}

pub trait TraceSampler: Send + Sync {
    fn should_sample(&self, headers: &http::HeaderMap, url: &str) -> SamplingDecision;
    fn update_config(&self, config: SamplerConfig);
}

pub struct DefaultSampler {
    config: parking_lot::RwLock<SamplerConfig>,
    regexes: parking_lot::RwLock<Vec<Regex>>,
}

impl DefaultSampler {
    pub fn new(config: SamplerConfig) -> Arc<Self> {
        let regexes = Self::compile_patterns(&config.url_patterns);
        Arc::new(Self {
            config: parking_lot::RwLock::new(config),
            regexes: parking_lot::RwLock::new(regexes),
        })
    }

    fn compile_patterns(patterns: &[String]) -> Vec<Regex> {
        patterns
            .iter()
            .filter_map(|p| Regex::new(p).ok())
            .collect()
    }

    fn generate_trace_id() -> String {
        use rand::RngCore;
        let mut bytes = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut bytes);
        hex_encode(&bytes)
    }

    fn hash_url(url: &str) -> u64 {
        let mut hasher = XxHash64::default();
        url.hash(&mut hasher);
        hasher.finish()
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

impl TraceSampler for DefaultSampler {
    fn should_sample(&self, headers: &http::HeaderMap, url: &str) -> SamplingDecision {
        let trace_id = headers
            .get(TRACE_ID_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .unwrap_or_else(|| Self::generate_trace_id());

        if let Some(sampled_header) = headers.get(SAMPLED_HEADER) {
            if let Ok(val) = sampled_header.to_str() {
                if val == "1" || val.eq_ignore_ascii_case("true") {
                    return SamplingDecision {
                        sampled: true,
                        reason: SamplingReason::HeaderForced,
                        trace_id,
                    };
                }
                if val == "0" || val.eq_ignore_ascii_case("false") {
                    return SamplingDecision {
                        sampled: false,
                        reason: SamplingReason::NotSampled,
                        trace_id,
                    };
                }
            }
        }

        let config = self.config.read();

        if config.force_canary {
            if let Some(canary_val) = headers.get(CANARY_HEADER) {
                if let Ok(val) = canary_val.to_str() {
                    if val.eq_ignore_ascii_case("canary") {
                        return SamplingDecision {
                            sampled: true,
                            reason: SamplingReason::CanaryForced,
                            trace_id,
                        };
                    }
                }
            }
        }

        {
            let regexes = self.regexes.read();
            for regex in regexes.iter() {
                if regex.is_match(url) {
                    return SamplingDecision {
                        sampled: true,
                        reason: SamplingReason::UrlRegex,
                        trace_id,
                    };
                }
            }
        }

        if config.percentage > 0.0 {
            let percentage = config.percentage.clamp(0.0, 1.0);

            let hash = Self::hash_url(&format!("{}|{}", trace_id, url));
            let threshold = (percentage * u64::MAX as f64) as u64;

            if hash < threshold {
                return SamplingDecision {
                    sampled: true,
                    reason: SamplingReason::Percentage,
                    trace_id,
                };
            }

            let mut rng = rand::thread_rng();
            if rng.gen::<f64>() < percentage * 0.1 {
                return SamplingDecision {
                    sampled: true,
                    reason: SamplingReason::Percentage,
                    trace_id,
                };
            }
        }

        SamplingDecision {
            sampled: false,
            reason: SamplingReason::NotSampled,
            trace_id,
        }
    }

    fn update_config(&self, config: SamplerConfig) {
        let regexes = Self::compile_patterns(&config.url_patterns);
        *self.config.write() = config;
        *self.regexes.write() = regexes;
    }
}

pub fn inject_sampling_headers(decision: &SamplingDecision, headers: &mut http::HeaderMap) {
    if let Ok(value) = http::HeaderValue::from_str(&decision.trace_id) {
        headers.insert(TRACE_ID_HEADER, value);
    }

    let sampled_value = if decision.sampled { "1" } else { "0" };
    headers.insert(SAMPLED_HEADER, http::HeaderValue::from_static(sampled_value));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_canary_force_sampling() {
        let config = SamplerConfig {
            percentage: 0.0,
            force_canary: true,
            url_patterns: vec![],
        };
        let sampler = DefaultSampler::new(config);

        let mut headers = http::HeaderMap::new();
        headers.insert(CANARY_HEADER, http::HeaderValue::from_static("canary"));

        let decision = sampler.should_sample(&headers, "/api/test");
        assert!(decision.sampled);
        assert_eq!(decision.reason, SamplingReason::CanaryForced);
    }

    #[test]
    fn test_url_regex_sampling() {
        let config = SamplerConfig {
            percentage: 0.0,
            force_canary: false,
            url_patterns: vec![r"^/api/v1/orders/.*".to_string()],
        };
        let sampler = DefaultSampler::new(config);

        let headers = http::HeaderMap::new();

        let decision = sampler.should_sample(&headers, "/api/v1/orders/123");
        assert!(decision.sampled);
        assert_eq!(decision.reason, SamplingReason::UrlRegex);

        let decision = sampler.should_sample(&headers, "/api/v1/users/123");
        assert!(!decision.sampled);
    }

    #[test]
    fn test_header_forced_sampling() {
        let config = SamplerConfig::default();
        let sampler = DefaultSampler::new(config);

        let mut headers = http::HeaderMap::new();
        headers.insert(SAMPLED_HEADER, http::HeaderValue::from_static("1"));

        let decision = sampler.should_sample(&headers, "/any/url");
        assert!(decision.sampled);
        assert_eq!(decision.reason, SamplingReason::HeaderForced);
    }

    #[test]
    fn test_header_forced_not_sampled() {
        let config = SamplerConfig {
            percentage: 1.0,
            force_canary: true,
            url_patterns: vec![],
        };
        let sampler = DefaultSampler::new(config);

        let mut headers = http::HeaderMap::new();
        headers.insert(CANARY_HEADER, http::HeaderValue::from_static("canary"));
        headers.insert(SAMPLED_HEADER, http::HeaderValue::from_static("0"));

        let decision = sampler.should_sample(&headers, "/any/url");
        assert!(!decision.sampled);
        assert_eq!(decision.reason, SamplingReason::NotSampled);
    }

    #[test]
    fn test_percentage_sampling() {
        let config = SamplerConfig {
            percentage: 1.0,
            force_canary: false,
            url_patterns: vec![],
        };
        let sampler = DefaultSampler::new(config);

        let headers = http::HeaderMap::new();

        let mut sampled_count = 0;
        for _ in 0..100 {
            let decision = sampler.should_sample(&headers, "/test/url");
            if decision.sampled {
                sampled_count += 1;
            }
        }
        assert!(sampled_count > 50, "Expected majority sampled at 100% config");
    }

    #[test]
    fn test_trace_id_generation() {
        let sampler = DefaultSampler::new(SamplerConfig::default());
        let headers = http::HeaderMap::new();

        let d1 = sampler.should_sample(&headers, "/a");
        let d2 = sampler.should_sample(&headers, "/b");

        assert_ne!(d1.trace_id, d2.trace_id);
        assert_eq!(d1.trace_id.len(), 32);
    }

    #[test]
    fn test_trace_id_propagation() {
        let sampler = DefaultSampler::new(SamplerConfig::default());

        let mut headers = http::HeaderMap::new();
        headers.insert(
            TRACE_ID_HEADER,
            http::HeaderValue::from_static("abcdef1234567890abcdef1234567890"),
        );

        let decision = sampler.should_sample(&headers, "/test");
        assert_eq!(decision.trace_id, "abcdef1234567890abcdef1234567890");
    }
}
