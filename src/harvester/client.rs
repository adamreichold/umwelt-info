use std::fmt;
use std::future::Future;

use anyhow::{Error, Result};
use bytes::Bytes;
use reqwest::Client as HttpClient;
use tokio::time::{sleep, Duration};

#[derive(Clone)]
pub struct Client {
    http_client: HttpClient,
}

impl Client {
    pub fn start() -> Result<Self> {
        let http_client = HttpClient::builder()
            .user_agent("umwelt.info harvester")
            .timeout(Duration::from_secs(300))
            .build()?;

        Ok(Self { http_client })
    }

    pub async fn make_request<'a, A, F, T, E>(&'a self, mut action: A) -> Result<T>
    where
        A: FnMut(&'a HttpClient) -> F,
        F: Future<Output = Result<T, E>>,
        T: Response,
        E: Into<Error> + fmt::Display,
    {
        retry_request(|| action(&self.http_client)).await
    }
}

pub trait Response {}

impl Response for Bytes {}

impl Response for String {}

async fn retry_request<A, F, T, E>(mut action: A) -> Result<T>
where
    A: FnMut() -> F,
    F: Future<Output = Result<T, E>>,
    T: Response,
    E: Into<Error> + fmt::Display,
{
    let mut attempts = 0;
    let mut duration = Duration::from_secs(1);

    loop {
        match action().await {
            Ok(val) => return Ok(val),
            Err(err) => {
                if attempts < 3 {
                    tracing::warn!("Request failed but will be retried: {:#}", err);

                    sleep(duration).await;

                    attempts += 1;
                    duration *= 10;
                } else {
                    return Err(err.into());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use anyhow::anyhow;
    use tokio::time::{pause, Instant};

    #[tokio::test]
    async fn retry_request_fowards_success() {
        pause();
        let start = Instant::now();

        retry_request::<_, _, _, Error>(|| async { Ok(Bytes::new()) })
            .await
            .unwrap();

        assert_eq!(start.elapsed().as_secs(), 0);
    }

    #[tokio::test]
    async fn retry_request_fowards_failure() {
        pause();
        let start = Instant::now();

        retry_request::<_, _, Bytes, _>(|| async { Err(anyhow!("failure")) })
            .await
            .unwrap_err();

        assert_eq!(start.elapsed().as_secs(), 1 + 10 + 100);
    }

    #[tokio::test]
    async fn retry_request_retries_three_times() {
        pause();
        let start = Instant::now();

        let mut count = 0;

        retry_request(|| {
            count += 1;

            async move {
                if count > 3 {
                    Ok(Bytes::new())
                } else {
                    Err(anyhow!("failure"))
                }
            }
        })
        .await
        .unwrap();

        assert_eq!(start.elapsed().as_secs(), 1 + 10 + 100);
    }
}
