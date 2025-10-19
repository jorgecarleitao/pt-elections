use reqwest::{self, Client, StatusCode};
use reqwest_middleware::ClientBuilder;
use reqwest_retry::RetryTransientMiddleware;
use reqwest_retry::policies::ExponentialBackoff;

pub trait Source {
    fn name() -> &'static str;
    type KeyType: std::fmt::Debug + std::fmt::Display;
    async fn keys() -> impl Iterator<Item = Self::KeyType>;
    fn request(key: &Self::KeyType) -> reqwest::Request;
}

#[tracing::instrument]
pub async fn fetch(request: reqwest::Request) -> Result<Vec<u8>, std::io::Error> {
    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(5);
    let client = ClientBuilder::new(Client::new())
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build();

    let response = client
        .execute(request)
        .await
        .map_err(std::io::Error::other)?;
    if response.status() == StatusCode::OK {
        Ok(response
            .bytes()
            .await
            .map_err(std::io::Error::other)?
            .to_vec())
    } else {
        tracing::error!("Request failed with status: {}", response.status());
        Err(std::io::Error::other(response.text().await.map_err(std::io::Error::other)?).into())
    }
}

#[tracing::instrument(skip(storage))]
pub async fn cached<S: Source>(
    key: &S::KeyType,
    storage: &dyn fs::BlobStorageProvider,
) -> Result<Vec<u8>, std::io::Error> {
    fs::cached(
        &format!("{}/{}.json", S::name(), key),
        async move { fetch(S::request(key)).await },
        storage,
        fs::CacheAction::ReadFetchWrite,
    )
    .await
}
