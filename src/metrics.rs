use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::errors::SetupFailure;

pub trait Metricer {
    fn encode(&self) -> Vec<u8>;
}

#[async_trait]
pub trait MetricProcessor {
    async fn setup(&mut self) -> Result<(), SetupFailure>;
    async fn add(&mut self, m: &(dyn Metricer)) -> Result<(), SetupFailure>;
    async fn end(&self);
}

pub enum Metric {
    Counter(String),
}

impl Metricer for Metric {
    fn encode(&self) -> Vec<u8> {
        match self {
            Metric::Counter(key) => format!("metric:counter:{}", key).into_bytes(),
        }
    }
}
pub struct MetricsProducerStdout {
    handle: tokio::io::Stdout,
    counters: HashMap<String, u64>,
}

impl MetricsProducerStdout {
    pub async fn new() -> Self {
        Self {
            handle: tokio::io::stdout(),
            counters: HashMap::new(),
        }
    }
}

#[async_trait]
impl MetricProcessor for MetricsProducerStdout {
    async fn setup(&mut self) -> Result<(), SetupFailure> {
        Ok(())
    }

    async fn add(&mut self, _m: &(dyn Metricer)) -> Result<(), SetupFailure> {
        Ok(())
    }

    async fn end(&self) {}
}

pub struct MetricExecutorTimings {
    pub setup_start: DateTime<Utc>,
    pub setup_finish: Option<DateTime<Utc>>,
    pub end_start: Option<DateTime<Utc>>,
    pub end_finish: Option<DateTime<Utc>>,
}
impl Default for MetricExecutorTimings {
    fn default() -> Self {
        Self {
            setup_start: Utc::now(),
            setup_finish: None,
            end_start: None,
            end_finish: None,
        }
    }
}

pub struct MetricAppTimings {
    pub setup_start: DateTime<Utc>,
    pub setup_finish: Option<DateTime<Utc>>,
    pub main_start: Option<DateTime<Utc>>,
    pub main_finish: Option<DateTime<Utc>>,
    pub end_start: Option<DateTime<Utc>>,
    pub end_finish: Option<DateTime<Utc>>,
}

impl Default for MetricAppTimings {
    fn default() -> Self {
        Self {
            setup_start: Utc::now(),
            setup_finish: None,
            main_start: None,
            main_finish: None,
            end_start: None,
            end_finish: None,
        }
    }
}
