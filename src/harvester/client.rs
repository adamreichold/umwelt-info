use std::env::var;
use std::fmt;
use std::future::Future;
use std::sync::Arc;

use anyhow::{Error, Result};
use async_compression::tokio::{bufread::ZstdDecoder, write::ZstdEncoder};
use bytes::Bytes;
use cap_std::fs::Dir;
use reqwest::Client as HttpClient;
use tokio::time::{sleep, Duration};
use tokio::{
    fs::File as AsyncFile,
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
};

#[derive(Clone)]
pub struct Client {
    replay: bool,
    http_client: HttpClient,
    dir: Arc<Dir>,
}

impl Client {
    pub fn start(dir: &Dir) -> Result<Self> {
        let replay = var("REPLAY_RESPONSES").is_ok();

        let http_client = HttpClient::builder()
            .user_agent("umwelt.info harvester")
            .timeout(Duration::from_secs(300))
            .build()?;

        if !replay {
            let _ = dir.remove_dir_all("responses");
        }

        dir.create_dir_all("responses")?;

        let dir = Arc::new(dir.open_dir("responses")?);

        Ok(Self {
            replay,
            dir,
            http_client,
        })
    }

    pub async fn make_request<'a, A, F, T, E>(&'a self, key: &str, mut action: A) -> Result<T>
    where
        A: FnMut(&'a HttpClient) -> F,
        F: Future<Output = Result<T, E>>,
        T: Response,
        E: Into<Error> + fmt::Display,
    {
        if self.replay {
            if let Ok(file) = self.dir.open(key) {
                let mut file =
                    ZstdDecoder::new(BufReader::new(AsyncFile::from_std(file.into_std())));

                let mut buf = Vec::new();
                file.read_to_end(&mut buf).await?;

                return T::from_buf(buf);
            } else {
                tracing::warn!("Failed to replay {key}");
            }
        }

        let response = retry_request(|| action(&self.http_client)).await?;

        let file = self.dir.create(key)?;

        let mut file = ZstdEncoder::new(AsyncFile::from_std(file.into_std()));
        file.write_all(response.as_ref()).await?;
        file.shutdown().await?;

        Ok(response)
    }
}

pub trait Response: AsRef<[u8]> + Sized {
    fn from_buf(buf: Vec<u8>) -> Result<Self>;
}

impl Response for Bytes {
    fn from_buf(buf: Vec<u8>) -> Result<Self> {
        Ok(buf.into())
    }
}

impl Response for String {
    fn from_buf(buf: Vec<u8>) -> Result<Self> {
        let text = String::from_utf8(buf)?;

        Ok(text)
    }
}

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
