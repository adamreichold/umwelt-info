use std::future::{ready, Ready};
use std::time::Instant;

use anyhow::Result;
use axum::{extract::MatchedPath, http::Request, middleware::Next, response::Response};
use metrics::{describe_histogram, histogram, Unit};
use metrics_exporter_prometheus::PrometheusBuilder;

pub fn install_recorder() -> Result<impl FnOnce() -> Ready<String> + Clone + Send> {
    let handle = PrometheusBuilder::new().install_recorder()?;
    let render = move || ready(handle.render());

    describe_histogram!(
        "request_duration",
        Unit::Seconds,
        "Summary of request count and duration by route"
    );

    Ok(render)
}

pub async fn measure_routes<B>(path: MatchedPath, req: Request<B>, next: Next<B>) -> Response {
    let start = Instant::now();

    let resp = next.run(req).await;

    let elapsed = start.elapsed().as_secs_f64();
    let path = path.as_str().to_owned();

    histogram!("request_duration", elapsed, &[("route", path)]);

    resp
}
