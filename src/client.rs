use opentelemetry::{trace::TracerProvider as _, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    resource::Resource,
    trace::SdkTracerProvider,
};
use uuid::Uuid;

use crate::handles::TaskHandle;

const SDK_VERSION: &str = "0.2.0";

#[derive(Debug, Clone, Default)]
pub struct RouteIQOptions {
    pub agent_id: String,
    pub otlp_endpoint: Option<String>,
    pub tenant_id: Option<String>,
    pub model: Option<String>,
    pub environment: Option<String>,
    pub agent_version: Option<String>,
    pub api_key: Option<String>,
}

pub struct RouteIQ {
    pub(crate) agent_id: String,
    pub(crate) tenant_id: String,
    pub(crate) model: Option<String>,
    pub(crate) environment: String,
    pub(crate) agent_version: String,
    pub session_id: String,
    pub(crate) provider: SdkTracerProvider,
}

impl RouteIQ {
    pub fn new(opts: RouteIQOptions) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let endpoint = opts
            .otlp_endpoint
            .clone()
            .unwrap_or_else(|| "http://localhost:4317".to_string());

        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()?;

        let resource = Resource::builder()
            .with_attribute(KeyValue::new("service.name", opts.agent_id.clone()))
            .with_attribute(KeyValue::new("routeiq.sdk.version", SDK_VERSION))
            .build();

        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(resource)
            .build();

        Ok(Self::with_provider(opts, provider))
    }

    pub fn with_provider(opts: RouteIQOptions, provider: SdkTracerProvider) -> Self {
        RouteIQ {
            agent_id: opts.agent_id,
            tenant_id: opts.tenant_id.unwrap_or_else(|| "default".to_string()),
            model: opts.model,
            environment: opts.environment.unwrap_or_else(|| "production".to_string()),
            agent_version: opts.agent_version.unwrap_or_else(|| "1.0.0".to_string()),
            session_id: Uuid::new_v4().to_string(),
            provider,
        }
    }

    pub fn task(&self, intent: impl Into<String>) -> TaskHandle {
        TaskHandle::new(self, intent.into(), None)
    }

    pub fn task_typed(&self, intent: impl Into<String>, task_type: impl Into<String>) -> TaskHandle {
        TaskHandle::new(self, intent.into(), Some(task_type.into()))
    }

    pub fn flush(&self) {
        self.provider.force_flush();
    }

    pub(crate) fn tracer(&self) -> opentelemetry_sdk::trace::Tracer {
        self.provider.tracer("routeiq.sdk")
    }

    pub(crate) fn envelope_attrs(&self, task: Option<&TaskHandle>, step_id: Option<&str>) -> Vec<KeyValue> {
        let mut attrs = vec![
            KeyValue::new("routeiq.agent.id",    self.agent_id.clone()),
            KeyValue::new("routeiq.tenant.id",   self.tenant_id.clone()),
            KeyValue::new("routeiq.environment", self.environment.clone()),
            KeyValue::new("routeiq.session.id",  self.session_id.clone()),
        ];
        if let Some(t) = task {
            attrs.push(KeyValue::new("routeiq.task.id", t.task_id.clone()));
            attrs.push(KeyValue::new("routeiq.run.id",  t.run_id.clone()));
        }
        if let Some(sid) = step_id {
            attrs.push(KeyValue::new("routeiq.step.id", sid.to_string()));
        }
        if let Some(ref m) = self.model {
            attrs.push(KeyValue::new("routeiq.version.model.name", m.clone()));
        }
        attrs.push(KeyValue::new("routeiq.version.agent", self.agent_version.clone()));
        attrs
    }
}
