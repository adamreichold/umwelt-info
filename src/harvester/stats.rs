use std::env::var;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Result;
use askama::Template;
use lettre::{
    message::{header::ContentType, Mailbox, SinglePart},
    AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor,
};

#[derive(Default, Debug)]
pub struct Stats(Mutex<Vec<StatsInner>>);

impl Stats {
    pub fn record_harvest(
        &self,
        source: String,
        duration: Duration,
        count: usize,
        transmitted: usize,
        failed: usize,
    ) {
        self.0.lock().unwrap().push(StatsInner {
            name: source,
            duration,
            count,
            transmitted,
            failed,
        });
    }

    pub async fn mail_summary(self, duration: Duration) -> Result<()> {
        let mail_server = match var("MAIL_SERVER") {
            Ok(mail_server) => mail_server,
            Err(_err) => return Ok(()),
        };

        let mail_from = var("MAIL_FROM")
            .expect("Environment variable MAIL_FROM not set")
            .parse::<Mailbox>()
            .expect("Environment variable MAIL_FROM invalid");

        let mail_to = var("MAIL_TO")
            .expect("Environment variable MAIL_TO not set")
            .parse::<Mailbox>()
            .expect("Environment variable MAIL_TO invalid");

        let sources = self.0.into_inner().unwrap();

        let (count, transmitted, failed) =
            sources
                .iter()
                .fold((0, 0, 0), |(count, transmitted, failed), source| {
                    (
                        count + source.count,
                        transmitted + source.transmitted,
                        failed + source.failed,
                    )
                });

        #[derive(Template)]
        #[template(path = "summary.html")]
        struct Summary {
            sources: Vec<StatsInner>,
            duration: Duration,
            count: usize,
            transmitted: usize,
            failed: usize,
        }

        let summary = Summary {
            sources,
            duration,
            count,
            transmitted,
            failed,
        }
        .render()
        .unwrap();

        let mail = Message::builder()
            .from(mail_from)
            .to(mail_to)
            .subject("umwelt.info: Zusammenfassung")
            .singlepart(
                SinglePart::builder()
                    .header(ContentType::TEXT_HTML)
                    .body(summary),
            )
            .unwrap();

        AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(mail_server)
            .build()
            .send(mail)
            .await?;

        Ok(())
    }
}

#[derive(Debug)]
struct StatsInner {
    name: String,
    duration: Duration,
    count: usize,
    transmitted: usize,
    failed: usize,
}

mod filters {
    use std::time::Duration;

    use askama::Result;

    pub fn duration(val: &Duration) -> Result<String> {
        let secs = val.as_secs();

        let val = if secs > 3600 {
            format!("{}h", secs / 3600)
        } else if secs > 60 {
            format!("{}min", secs / 60)
        } else {
            format!("{}s", secs)
        };

        Ok(val)
    }
}
