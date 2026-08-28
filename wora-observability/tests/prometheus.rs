#![cfg(feature = "prometheus")]

use prometheus::{Encoder, Registry, TextEncoder};
use wora::o11y::{O11ySink, RuntimeMetrics, o11y_new_ev_runtime_metrics};
use wora::{HealthState, Leadership, ReadinessState};
use wora_observability::prometheus::PrometheusSink;

#[tokio::test]
async fn runtime_snapshot_updates_registered_metrics() -> Result<(), Box<dyn std::error::Error>> {
    let registry = Registry::new();
    let mut sink = PrometheusSink::builder(registry.clone()).static_label("app", "demo").build()?;
    let event = o11y_new_ev_runtime_metrics::<()>(&RuntimeMetrics {
        app_name: "demo".into(),
        pid: 42,
        leadership: Leadership::Leader,
        health: HealthState::Ok,
        readiness: ReadinessState::Ready,
        restart_count: 3,
        event_backlog_capacity: 7,
        event_backlog_max_capacity: 16,
    });
    sink.handle_event(&event).await?;
    sink.handle_event(&event).await?;
    let mut output = Vec::new();
    TextEncoder::new().encode(&registry.gather(), &mut output)?;
    let text = String::from_utf8(output)?;
    assert!(text.contains("wora_runtime_restart_count{app=\"demo\"} 3"));
    assert!(text.contains("wora_runtime_health_state{app=\"demo\"} 1"));
    assert!(text.contains("wora_runtime_leadership_state{app=\"demo\"} 2"));
    assert!(text.contains("wora_o11y_events_total{app=\"demo\",kind=\"runtime_metrics\"} 2"));
    Ok(())
}
