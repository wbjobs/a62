use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterType {
    Stable,
    Canary,
}

impl fmt::Display for ClusterType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClusterType::Stable => write!(f, "stable"),
            ClusterType::Canary => write!(f, "canary"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RouteTarget {
    pub service_name: String,
    pub cluster_type: ClusterType,
    pub endpoint: String,
}

pub const CANARY_HEADER: &str = "x-envoy";
pub const CANARY_HEADER_VALUE: &str = "canary";
pub const CANARY_CLUSTER_HEADER: &str = "x-canary-cluster";
pub const GLOBAL_TIMEOUT_HEADER: &str = "x-global-timeout-remaining-ms";
