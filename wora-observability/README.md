# wora-observability

Optional Prometheus and OpenTelemetry metric exporters for `wora::o11y::O11yEvent`.

Enable only the backend you use:

```toml
wora-observability = { version = "0.0.1", features = ["prometheus-http"] }
```

```rust,no_run
use prometheus::Registry;
use wora_observability::prometheus::PrometheusSink;

let registry = Registry::new();
let sink = PrometheusSink::builder(registry.clone())
    .prefix("wora")
    .static_label("app", "example")
    .build()?;
let pipeline = wora::o11y::O11yPipeline::builder().sink("prometheus", sink).build()?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

Prometheus state values are stable integers: health is unknown=0, ok=1,
suspended=2, try_again=3, failed=4; readiness is unknown=0, not_ready=1,
ready=2, stopping=3, draining=4; leadership is unknown=0, follower=1, leader=2.

The `prometheus-http` feature provides `serve_prometheus_metrics`, which serves
the supplied registry at `/metrics`. The `opentelemetry` feature accepts an
application-provided meter, while `otlp` provides `OpenTelemetryPipelineBuilder`.

Run the daemon example and scrape its endpoint with:

```sh
cargo run -p wora-observability --example async_daemon_prometheus \
  --features prometheus-http -- --run-mode user
curl http://127.0.0.1:9090/metrics
```

The example keeps the pipeline's control handle inside the application. Its
configuration can change sampling and flush intervals or pause Prometheus
updates without restarting the daemon:

```toml
[observability]
flush_secs = 10
status_secs = 15
host_stats_secs = 30
prometheus_enabled = true
```

Writing that configuration to the daemon's main metadata file and triggering
a configuration reload applies all interval changes atomically.

Named sinks can also be controlled and inspected directly from an app:

```rust,no_run
# use wora::prelude::*;
# async fn update(control: &O11yControlHandle) -> Result<(), O11yControlError> {
control.disable_sink("prometheus").await?;
control
    .apply(O11ySettingsPatch::new().host_stats_interval(std::time::Duration::from_secs(30)))
    .await?;
let status = control.status().await?;
println!("{:?}", status.sinks["prometheus"].state);
println!("{:?}", status.services["Prometheus HTTP endpoint"].state);
# Ok(())
# }
```
