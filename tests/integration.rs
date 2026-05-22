use opentelemetry::Context;
use opentelemetry_sdk::{
    error::OTelSdkResult,
    trace::{SdkTracerProvider, SpanData, SpanProcessor},
};
use routeiq_sdk::{CompleteOpts, RouteIQ, RouteIQOptions, ToolOpts};
use std::sync::{Arc, Mutex};

// ── SpanRecorder ──────────────────────────────────────────────────────────────

struct SpanRecorder {
    spans: Arc<Mutex<Vec<SpanData>>>,
}

impl std::fmt::Debug for SpanRecorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SpanRecorder")
    }
}

impl Clone for SpanRecorder {
    fn clone(&self) -> Self {
        SpanRecorder { spans: self.spans.clone() }
    }
}

impl Default for SpanRecorder {
    fn default() -> Self {
        SpanRecorder { spans: Arc::new(Mutex::new(Vec::new())) }
    }
}

impl SpanProcessor for SpanRecorder {
    fn on_start(&self, _span: &mut opentelemetry_sdk::trace::Span, _cx: &Context) {}

    fn on_end(&self, span: SpanData) {
        self.spans.lock().unwrap().push(span);
    }

    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    fn shutdown(&self) -> OTelSdkResult {
        Ok(())
    }
}

// ── Test setup ────────────────────────────────────────────────────────────────

fn make_test_riq() -> (RouteIQ, SpanRecorder) {
    let recorder = SpanRecorder::default();
    let provider = SdkTracerProvider::builder()
        .with_span_processor(recorder.clone())
        .build();
    let opts = RouteIQOptions {
        agent_id: "test-agent".to_string(),
        tenant_id: Some("test-tenant".to_string()),
        environment: Some("test".to_string()),
        model: Some("gpt-4o".to_string()),
        agent_version: Some("1.0.0".to_string()),
        ..Default::default()
    };
    (RouteIQ::with_provider(opts, provider), recorder)
}

fn attr_str(span: &SpanData, key: &str) -> String {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .map(|kv| kv.value.as_str().to_string())
        .unwrap_or_default()
}

fn attr_i64(span: &SpanData, key: &str) -> i64 {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .and_then(|kv| {
            if let opentelemetry::Value::I64(v) = &kv.value { Some(*v) } else { None }
        })
        .unwrap_or(0)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[test]
fn test_task_span_name() {
    let (riq, rec) = make_test_riq();
    riq.task("find Paris").end();
    let spans = rec.spans.lock().unwrap();
    assert!(spans.iter().any(|s| s.name.starts_with("task:")), "expected task: span");
}

#[test]
fn test_task_envelope_attrs() {
    let (riq, rec) = make_test_riq();
    let session_id = riq.session_id.clone();
    let task = riq.task("find Paris");
    let task_id = task.task_id.clone();
    task.end();

    let spans = rec.spans.lock().unwrap();
    let span = spans.iter().find(|s| s.name.starts_with("task:")).unwrap();
    assert_eq!(attr_str(span, "routeiq.agent.id"), "test-agent");
    assert_eq!(attr_str(span, "routeiq.session.id"), session_id);
    assert_eq!(attr_str(span, "routeiq.task.id"), task_id);
    assert_eq!(attr_str(span, "routeiq.task.input_intent"), "find Paris");
    assert_eq!(attr_str(span, "routeiq.version.model.name"), "gpt-4o");
}

#[test]
fn test_task_complete() {
    let (riq, rec) = make_test_riq();
    let mut task = riq.task("q");
    task.complete(CompleteOpts { tokens: 100, cohort: Some("test".to_string()), ..Default::default() });
    task.end();

    let spans = rec.spans.lock().unwrap();
    let span = spans.iter().find(|s| s.name.starts_with("task:")).unwrap();
    assert_eq!(attr_str(span, "routeiq.task.completion_status"), "1");
    assert_eq!(attr_i64(span, "routeiq.task.total_tokens"), 100);
    assert_eq!(attr_str(span, "routeiq.task.cohort"), "test");
}

#[test]
fn test_task_fail() {
    let (riq, rec) = make_test_riq();
    let mut task = riq.task("q");
    task.fail(Some("tool_error"));
    task.end();

    let spans = rec.spans.lock().unwrap();
    let span = spans.iter().find(|s| s.name.starts_with("task:")).unwrap();
    assert_eq!(attr_str(span, "routeiq.task.completion_status"), "2");
    assert_eq!(attr_str(span, "routeiq.task.failure_category"), "tool_error");
}

#[test]
fn test_task_auto_complete() {
    let (riq, rec) = make_test_riq();
    riq.task("q").end();

    let spans = rec.spans.lock().unwrap();
    let span = spans.iter().find(|s| s.name.starts_with("task:")).unwrap();
    assert_eq!(attr_str(span, "routeiq.task.completion_status"), "1");
}

#[test]
fn test_step_span_name() {
    let (riq, rec) = make_test_riq();
    let mut task = riq.task("q");
    task.step(Some("tool_call"), None).end();
    task.end();

    let spans = rec.spans.lock().unwrap();
    assert!(spans.iter().any(|s| s.name.starts_with("step:")), "expected step: span");
}

#[test]
fn test_step_carries_task_id() {
    let (riq, rec) = make_test_riq();
    let mut task = riq.task("q");
    let task_id = task.task_id.clone();
    let step = task.step(None, None);
    let step_id = step.step_id.clone();
    step.end();
    task.end();

    let spans = rec.spans.lock().unwrap();
    let span = spans.iter().find(|s| s.name.starts_with("step:")).unwrap();
    assert_eq!(attr_str(span, "routeiq.task.id"), task_id);
    assert_eq!(attr_str(span, "routeiq.step.id"), step_id);
}

#[test]
fn test_step_index_increments() {
    let (riq, rec) = make_test_riq();
    let mut task = riq.task("q");
    task.step(None, None).end();
    task.step(None, None).end();
    task.end();

    let spans = rec.spans.lock().unwrap();
    let mut indices: Vec<i64> = spans
        .iter()
        .filter(|s| s.name.starts_with("step:"))
        .map(|s| attr_i64(s, "routeiq.step.index"))
        .collect();
    indices.sort();
    assert_eq!(indices, vec![1, 2]);
}

#[test]
fn test_tool_span_name() {
    let (riq, rec) = make_test_riq();
    let mut task = riq.task("q");
    let step = task.step(None, None);
    step.tool("search", ToolOpts::default()).end();
    step.end();
    task.end();

    let spans = rec.spans.lock().unwrap();
    assert!(spans.iter().any(|s| s.name == "tool:search"));
}

#[test]
fn test_tool_success() {
    let (riq, rec) = make_test_riq();
    let mut task = riq.task("q");
    let step = task.step(None, None);
    let mut tool = step.tool("search", ToolOpts::default());
    tool.success(Some(50.0));
    tool.end();
    step.end();
    task.end();

    let spans = rec.spans.lock().unwrap();
    let span = spans.iter().find(|s| s.name == "tool:search").unwrap();
    assert_eq!(attr_str(span, "routeiq.tool.result_status"), "1");
}

#[test]
fn test_tool_fail() {
    let (riq, rec) = make_test_riq();
    let mut task = riq.task("q");
    let step = task.step(None, None);
    let mut tool = step.tool("search", ToolOpts::default());
    tool.fail(Some("TIMEOUT"), None);
    tool.end();
    step.end();
    task.end();

    let spans = rec.spans.lock().unwrap();
    let span = spans.iter().find(|s| s.name == "tool:search").unwrap();
    assert_eq!(attr_str(span, "routeiq.tool.result_status"), "2");
    assert_eq!(attr_str(span, "routeiq.tool.error_code"), "TIMEOUT");
}

#[test]
fn test_tool_auto_success() {
    let (riq, rec) = make_test_riq();
    let mut task = riq.task("q");
    let step = task.step(None, None);
    step.tool("search", ToolOpts::default()).end();
    step.end();
    task.end();

    let spans = rec.spans.lock().unwrap();
    let span = spans.iter().find(|s| s.name == "tool:search").unwrap();
    assert_eq!(attr_str(span, "routeiq.tool.result_status"), "1");
}

#[test]
fn test_tool_args_hash() {
    let (riq, rec) = make_test_riq();
    let mut task = riq.task("q");
    let step = task.step(None, None);
    let opts = ToolOpts {
        args: Some(serde_json::json!({"query": "Paris"})),
        ..Default::default()
    };
    step.tool("search", opts).end();
    step.end();
    task.end();

    let spans = rec.spans.lock().unwrap();
    let span = spans.iter().find(|s| s.name == "tool:search").unwrap();
    let h = attr_str(span, "routeiq.tool.arguments_hash");
    assert_eq!(h.len(), 16, "expected 16-char hex hash, got {h:?}");
}

#[test]
fn test_session_id_consistent() {
    let (riq, rec) = make_test_riq();
    let session_id = riq.session_id.clone();
    let mut task = riq.task("q");
    let step = task.step(None, None);
    step.tool("search", ToolOpts::default()).end();
    step.end();
    task.end();

    let spans = rec.spans.lock().unwrap();
    let ids: std::collections::HashSet<String> = spans
        .iter()
        .map(|s| attr_str(s, "routeiq.session.id"))
        .collect();
    assert_eq!(ids.len(), 1);
    assert!(ids.contains(&session_id));
}
