
use vector_config::configurable_component;
// use vector_core::event::metric::Bucket;
use vector_core::{config::LogNamespace, event::{metric::Sample, StatisticKind, metric::{Bucket, Quantile}}};
use chrono::Utc;

use crate::{
    config::{
        DataType, Input, TransformConfig, TransformContext,
        Output,
    },
    event::{
        metric::{Metric, MetricKind, MetricValue, MetricTags},
        Event, Value,
    },
    schema,
    transforms::{FunctionTransform, OutputBuffer, Transform},
};

/// Configuration for the `metric_metadata` transform.
#[configurable_component(transform("metric_metadata", "Convert log events to metric events."))]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct MetricsMetadataConfig {
    /// The interval between flushes, in milliseconds.
    ///
    /// During this time frame, metrics with the same series data (name, namespace, tags, and so on) are aggregated.
    #[serde(default = "default_interval_ms")]
    pub interval_ms: u64,
}

const fn default_interval_ms() -> u64 {
    10 * 1000
}
impl_generate_config_from_default!(MetricsMetadataConfig);

#[derive(Debug, Clone)]
pub struct MetricsMetadata {
    config: MetricsMetadataConfig,
}

#[async_trait::async_trait]
#[typetag::serde(name = "metric_metadata")]
impl TransformConfig for MetricsMetadataConfig {
    async fn build(&self, _context: &TransformContext) -> crate::Result<Transform> {
        Ok(Transform::function(MetricsMetadata::new(self.clone())))
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn outputs(&self, _: &schema::Definition, _: LogNamespace) -> Vec<Output> {
        vec![Output::default(DataType::Metric)]
    }

    fn enable_concurrency(&self) -> bool {
        true
    }
}

impl MetricsMetadata {
    pub const fn new(config: MetricsMetadataConfig) -> Self {
        MetricsMetadata { config }
    }
}

enum TransformError {
    MetricValueError {
        field: String
    }
}

fn to_metric(event: &Event) -> Result<Metric, TransformError> {
    let log = event.as_log();
    let timestamp = log
        .get_timestamp()
        .and_then(Value::as_timestamp)
        .cloned()
        .or_else(|| Some(Utc::now()));
    let metadata = event.metadata().clone();

    let binding = log.get("name").unwrap().to_string();
    let name = binding.trim_matches(|c| c == '\"');
    
    let namespace = match log.get("namespace") {
        Some(_value) => Some(log.get("namespace").unwrap().to_string()),
        None => Some("".to_string())
    };
    let tags = &mut MetricTags::default();

    if let Some(els) = log.get("tags") {
        if let Some(el) = els.as_object() {
            for (key, value) in el {
                tags.insert(String::from(key).to_string(), value.to_string().trim_matches(|c| c == '\"'));
            }
        }    
    }
    let tags_result = Some(tags.clone());
    let kind = match log.get("kind").unwrap().to_string().as_str() {
        "absolute" =>  MetricKind::Absolute,
        "incremental" => MetricKind::Incremental,
        _ => MetricKind::Absolute
    };

    let mut keys: Vec<&str> = Vec::new();
    if let Some(root_event) = log.as_map() {
        for (key, _v) in root_event {
            warn!(message = "Failed to merge value.", %key);
            keys.push(key.as_str());
        }
    }
   let mut value: Option<MetricValue> = None;
    for key in keys {
        value = match key {
            "gauge" => {
                Some(MetricValue::Gauge { value: *log.get("gauge.value").unwrap().as_float().unwrap() })
            },
            "distribution" => {
                let mut samples: Vec<Sample> = Vec::new();
                if let Some(event_samples) = log.get("distribution.samples").unwrap().as_array() {
                    for e_sample in event_samples {
                        samples.push(
                            Sample { value: *e_sample.get("value").unwrap().as_float().unwrap(), rate: e_sample.get("rate").unwrap().as_integer().unwrap() as u32 }
                        )
                    }
                }
                let statistic_kind = match log.get("distribution.statistic").unwrap().to_string().as_str() {
                    "histogram" =>  StatisticKind::Histogram,
                    "summary" => StatisticKind::Summary,
                    _ => StatisticKind::Histogram
                };
                Some(MetricValue::Distribution {
                    samples: samples,
                    statistic: statistic_kind 
                })
            },
            "histogram" => {
                let mut buckets: Vec<Bucket> = Vec::new();
                if let Some(event_buckets) = log.get("histogram.buckets").unwrap().as_array() {
                    for e_bucket in event_buckets {
                        buckets.push(
                            Bucket { 
                                upper_limit: *e_bucket.get("upper_limit").unwrap().as_float().unwrap(),
                                count: e_bucket.get("count").unwrap().as_integer().unwrap() as u64 
                            }
                        );
                    }
                }
                Some(MetricValue::AggregatedHistogram { 
                    buckets: buckets, 
                    count: log.get("histogram.count").unwrap().as_integer().unwrap() as u64, 
                    sum: *log.get("histogram.sum").unwrap().as_float().unwrap()
                })
            },
            "summary" => {
                let mut quantiles: Vec<Quantile> = Vec::new();
                if let Some(event_quantiles) = log.get("summary.quantiles").unwrap().as_array() {
                    for e_quantile in event_quantiles {
                        quantiles.push(
                            Quantile { 
                                quantile: *e_quantile.get("quantile").unwrap().as_float().unwrap(),
                                value: *e_quantile.get("value").unwrap().as_float().unwrap()
                            }
                        )
                    }
                }
                Some(MetricValue::AggregatedSummary { 
                    quantiles: quantiles, 
                    count: log.get("summary.count").unwrap().as_integer().unwrap() as u64, 
                    sum: *log.get("summary.sum").unwrap().as_float().unwrap() 
                })
            },
            _ => { value }
        }
    };
    if let Some(value) = value {
        Ok(Metric::new_with_metadata(name, kind, value, metadata)
        .with_namespace(namespace)
        .with_tags(tags_result)
        .with_timestamp(timestamp))
    } else {
        Err(TransformError::MetricValueError { field: "value".to_string() })
    }
    // let value = value.clone();
    
}

impl FunctionTransform for MetricsMetadata {
    fn transform(&mut self, output: &mut OutputBuffer, event: Event) {
        // Metrics are "all or none" for a specific log. If a single fails, none are produced.
        self.config.interval_ms = 12;
        match to_metric(&event) {
            Ok(metric) => {
                output.push(Event::Metric(metric));
            },
            Err(err) => {
                match err {
                    TransformError::MetricValueError { field } => {
                        error!(message = "Failed to create metric value ", %field);
                    }
                };
                // early return to prevent the partial buffer from being sent
                return;
            }
        }

        // Metric generation was successful, publish them all.
        // for event in buffer {
        //     output.push(event);
        // }
    }
}
