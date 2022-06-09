use gethostname::gethostname;
use opentelemetry::global::set_text_map_propagator;
use opentelemetry::sdk::propagation::TraceContextPropagator;
use opentelemetry::sdk::trace;
use opentelemetry::sdk::Resource;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use serde::Deserialize;
use std::collections::HashSet;
use thiserror::Error;
use tracing::info;
use tracing::subscriber::set_global_default;
use tracing::Subscriber;
use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
use tracing_log::LogTracer;
use tracing_subscriber::filter::FilterFn;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Layer;
use tracing_subscriber::{EnvFilter, Registry};

#[derive(Deserialize, Clone, Debug)]
pub struct TelemetrySetting {
    pub otel_collector_endpoint: String,
    pub disabled_targets: HashSet<String>,
    pub log_level: String,
}

impl TelemetrySetting {
    fn log_level_filter<S>(&self) -> impl Layer<S>
    where
        S: Subscriber,
    {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(self.log_level.clone()))
    }

    fn bunyan_formatter<S>(&self, service_name: &'static str) -> impl Layer<S>
    where
        S: Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
    {
        BunyanFormattingLayer::new(service_name.to_string(), std::io::stdout)
    }

    fn disable_targets_filter<S>(&self) -> impl Layer<S>
    where
        S: Subscriber,
    {
        let disabled_targets = self.disabled_targets.clone();
        FilterFn::new(move |metadata| !disabled_targets.contains(metadata.target()))
    }

    fn tracer<S>(&self, service_name: &'static str) -> impl Layer<S>
    where
        S: Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
    {
        let tracer = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_trace_config(trace::config().with_resource(Resource::new(vec![
                KeyValue::new("service.name", service_name),
                KeyValue::new("host.name", gethostname().into_string().unwrap()),
            ])))
            .with_exporter(
                opentelemetry_otlp::new_exporter()
                    .tonic()
                    .with_endpoint(self.otel_collector_endpoint.clone()),
            )
            .install_batch(opentelemetry::runtime::Tokio)
            .unwrap();

        tracing_opentelemetry::layer().with_tracer(tracer)
    }

    fn subscriber(&self, service_name: &'static str) -> impl Subscriber {
        Registry::default()
            .with(self.log_level_filter())
            .with(self.disable_targets_filter())
            .with(JsonStorageLayer)
            .with(self.bunyan_formatter(service_name))
            .with(self.tracer(service_name))
    }

    pub fn init_telemetry(&self, service_name: &'static str) -> Result<(), Error> {
        LogTracer::init().map_err(|_| Error::TelemetryAlreadyInit)?;
        set_text_map_propagator(TraceContextPropagator::new());
        set_global_default(self.subscriber(service_name))
            .map_err(|_| Error::TelemetryAlreadyInit)?;

        info!(
            "Initializing telemetry with log level [{}]: Done",
            self.log_level
        );

        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("telemetry already initialized")]
    TelemetryAlreadyInit,
}
