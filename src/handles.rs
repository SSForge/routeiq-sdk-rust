use opentelemetry::{
    trace::{Span, Tracer},
    KeyValue,
};
use sha2::{Digest, Sha256};
use std::time::Instant;
use uuid::Uuid;

use crate::client::RouteIQ;

const COMPLETION_SUCCESS: &str = "1";
const COMPLETION_FAILURE: &str = "2";
const TOOL_SUCCESS: &str = "1";
const TOOL_FAILURE: &str = "2";

fn permission_level(p: &str) -> &'static str {
    match p {
        "READ_WRITE" => "2",
        "PRIVILEGED" => "3",
        _ => "1",
    }
}

// ── CompleteOpts ──────────────────────────────────────────────────────────────

#[derive(Default)]
pub struct CompleteOpts {
    pub tokens: i64,
    pub cost_usd: Option<f64>,
    pub cohort: Option<String>,
}

// ── ToolOpts ──────────────────────────────────────────────────────────────────

#[derive(Default)]
pub struct ToolOpts {
    pub args: Option<serde_json::Value>,
    pub permission: Option<String>,
}

// ── ToolHandle ────────────────────────────────────────────────────────────────

pub struct ToolHandle {
    span: opentelemetry_sdk::trace::Span,
    start: Instant,
    done: bool,
}

impl ToolHandle {
    pub(crate) fn new(
        riq: &RouteIQ,
        task: &TaskHandle,
        step_id: &str,
        name: &str,
        opts: ToolOpts,
    ) -> Self {
        let args_hash = {
            let json = opts
                .args
                .as_ref()
                .map(|v| v.to_string())
                .unwrap_or_else(|| "{}".to_string());
            let mut hasher = Sha256::new();
            hasher.update(json.as_bytes());
            hex::encode(&hasher.finalize()[..8])
        };
        let perm = permission_level(
            opts.permission.as_deref().unwrap_or("READ_ONLY"),
        );

        let tracer = riq.tracer();
        let mut span = tracer.start(format!("tool:{name}"));
        let mut attrs = riq.envelope_attrs(Some(task), Some(step_id));
        attrs.extend([
            KeyValue::new("routeiq.event.type", "7"),
            KeyValue::new("routeiq.tool.name", name.to_string()),
            KeyValue::new("routeiq.tool.arguments_hash", args_hash),
            KeyValue::new("routeiq.tool.permission_level", perm),
        ]);
        span.set_attributes(attrs);

        ToolHandle { span, start: Instant::now(), done: false }
    }

    pub fn success(&mut self, latency_ms: Option<f64>) {
        self.finish(TOOL_SUCCESS, None, latency_ms);
    }

    pub fn fail(&mut self, error_code: Option<&str>, latency_ms: Option<f64>) {
        self.finish(TOOL_FAILURE, error_code, latency_ms);
    }

    fn finish(&mut self, status: &str, error_code: Option<&str>, latency_ms: Option<f64>) {
        if self.done { return; }
        self.done = true;
        let elapsed = self.start.elapsed().as_secs_f64() * 1000.0;
        let mut attrs = vec![
            KeyValue::new("routeiq.tool.result_status", status.to_string()),
            KeyValue::new("routeiq.tool.latency_ms", latency_ms.unwrap_or(elapsed)),
        ];
        if let Some(code) = error_code {
            attrs.push(KeyValue::new("routeiq.tool.error_code", code.to_string()));
        }
        self.span.set_attributes(attrs);
    }

    pub fn end(mut self) {
        if !self.done { self.success(None); }
        self.span.end();
    }
}

// ── StepHandle ────────────────────────────────────────────────────────────────

pub struct StepHandle<'task> {
    riq: &'task RouteIQ,
    task: &'task TaskHandle,
    pub step_id: String,
    span: opentelemetry_sdk::trace::Span,
    done: bool,
}

impl<'task> StepHandle<'task> {
    pub(crate) fn new(
        riq: &'task RouteIQ,
        task: &'task TaskHandle,
        action: Option<&str>,
        rationale: Option<&str>,
        index: i64,
    ) -> Self {
        let step_id = Uuid::new_v4().to_string();
        let tracer = riq.tracer();
        let mut span = tracer.start(format!("step:{step_id}"));

        let mut attrs = riq.envelope_attrs(Some(task), Some(&step_id));
        attrs.extend([
            KeyValue::new("routeiq.event.type", "4"),
            KeyValue::new("routeiq.step.index", index),
        ]);
        if let Some(a) = action {
            attrs.push(KeyValue::new("routeiq.step.selected_action", a.to_string()));
        }
        if let Some(r) = rationale {
            attrs.push(KeyValue::new("routeiq.step.action_rationale", r.to_string()));
        }
        span.set_attributes(attrs);

        StepHandle { riq, task, step_id, span, done: false }
    }

    pub fn tool(&self, name: &str, opts: ToolOpts) -> ToolHandle {
        ToolHandle::new(self.riq, self.task, &self.step_id, name, opts)
    }

    pub fn complete(&mut self) {
        self.finish(COMPLETION_SUCCESS, None);
    }

    pub fn fail(&mut self, category: Option<&str>) {
        self.finish(COMPLETION_FAILURE, category);
    }

    fn finish(&mut self, status: &str, category: Option<&str>) {
        if self.done { return; }
        self.done = true;
        let mut attrs = vec![KeyValue::new("routeiq.step.completion_status", status.to_string())];
        if let Some(cat) = category {
            attrs.push(KeyValue::new("routeiq.step.failure_category", cat.to_string()));
        }
        self.span.set_attributes(attrs);
    }

    pub fn end(mut self) {
        if !self.done { self.complete(); }
        self.span.end();
    }
}

// ── TaskHandle ────────────────────────────────────────────────────────────────

pub struct TaskHandle {
    riq: *const RouteIQ,
    pub task_id: String,
    pub run_id: String,
    span: opentelemetry_sdk::trace::Span,
    done: bool,
    step_index: i64,
}

impl TaskHandle {
    pub(crate) fn new(riq: &RouteIQ, intent: String, task_type: Option<String>) -> Self {
        let task_id = Uuid::new_v4().to_string();
        let run_id = Uuid::new_v4().to_string();

        let tracer = riq.tracer();
        let mut span = tracer.start(format!("task:{task_id}"));

        let mut attrs = riq.envelope_attrs(None, None);
        attrs.extend([
            KeyValue::new("routeiq.event.type", "1"),
            KeyValue::new("routeiq.task.id", task_id.clone()),
            KeyValue::new("routeiq.run.id", run_id.clone()),
            KeyValue::new("routeiq.task.input_intent", intent.chars().take(256).collect::<String>()),
        ]);
        if let Some(ref tt) = task_type {
            attrs.push(KeyValue::new("routeiq.task.type", tt.clone()));
        }
        span.set_attributes(attrs);

        TaskHandle { riq, task_id, run_id, span, done: false, step_index: 0 }
    }

    fn riq(&self) -> &RouteIQ {
        // Safety: RouteIQ outlives TaskHandle in all normal usage
        unsafe { &*self.riq }
    }

    pub fn step(&mut self, action: Option<&str>, rationale: Option<&str>) -> StepHandle<'_> {
        self.step_index += 1;
        StepHandle::new(self.riq(), self, action, rationale, self.step_index)
    }

    pub fn complete(&mut self, opts: CompleteOpts) {
        self.finish(COMPLETION_SUCCESS, opts, None);
    }

    pub fn fail(&mut self, category: Option<&str>) {
        self.finish(COMPLETION_FAILURE, CompleteOpts::default(), category);
    }

    fn finish(&mut self, status: &str, opts: CompleteOpts, failure_category: Option<&str>) {
        if self.done { return; }
        self.done = true;
        let mut attrs = vec![KeyValue::new("routeiq.task.completion_status", status.to_string())];
        if opts.tokens > 0 {
            attrs.push(KeyValue::new("routeiq.task.total_tokens", opts.tokens));
        }
        if let Some(cost) = opts.cost_usd {
            attrs.push(KeyValue::new("routeiq.task.cost_usd", cost));
        }
        if let Some(cohort) = opts.cohort {
            attrs.push(KeyValue::new("routeiq.task.cohort", cohort));
        }
        if let Some(cat) = failure_category {
            attrs.push(KeyValue::new("routeiq.task.failure_category", cat.to_string()));
        }
        self.span.set_attributes(attrs);
    }

    pub fn end(mut self) {
        if !self.done { self.complete(CompleteOpts::default()); }
        self.span.end();
    }
}
