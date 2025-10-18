use super::BlobStorageProvider;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheAction {
    ReadFetchWrite,
    ReadFetch,
    #[allow(dead_code)]
    FetchWrite,
}

impl CacheAction {
    pub fn from_date(date: &time::Date) -> Self {
        let now = time::OffsetDateTime::now_utc().date();
        (date >= &now)
            .then_some(Self::ReadFetch)
            .unwrap_or(Self::ReadFetchWrite)
    }
}

/// Tries to retrive `blob_name` from `provider`. If it does not exist,
/// it calls `fetch` and writes the result into `provider`.
/// Returns the data in `blob_name` from `provider`.
/// # Implementation
/// This function is idempotent but not pure.
pub async fn cached<E, F>(
    blob_name: &str,
    fetch: F,
    provider: &dyn BlobStorageProvider,
    action: CacheAction,
) -> Result<Vec<u8>, std::io::Error>
where
    E: std::error::Error + Send + Sync + 'static,
    F: futures::Future<Output = Result<Vec<u8>, E>>,
{
    match action {
        CacheAction::FetchWrite => miss(blob_name, fetch, provider, action).await,
        _ => {
            log::info!("Fetch {blob_name}");
            if let Some(data) = provider.maybe_get(blob_name).await? {
                log::info!("{blob_name} - cache hit");
                Ok(data)
            } else {
                miss(blob_name, fetch, provider, action).await
            }
        }
    }
}

/// Writes the result of `fetch` into `provider`.
/// Returns the result of fetch.
/// # Implementation
/// This function is idempotent and pure.
pub async fn miss<E, F>(
    blob_name: &str,
    fetch: F,
    provider: &dyn BlobStorageProvider,
    action: CacheAction,
) -> Result<Vec<u8>, std::io::Error>
where
    E: std::error::Error + Send + Sync + 'static,
    F: futures::Future<Output = Result<Vec<u8>, E>>,
{
    log::info!("{blob_name} - cache miss");
    let contents = fetch.await.map_err(std::io::Error::other)?;
    if action == CacheAction::ReadFetch || !provider.can_put() {
        log::info!("{blob_name} - cache do not write");
        return Ok(contents);
    };
    provider
        .put(blob_name, contents.clone())
        .await
        .map_err(|e| {
            log::error!("{blob_name} - put error {e}");
            e
        })?;
    log::info!("{blob_name} - cache write");
    Ok(contents)
}
