#[forbid(unsafe_code)]
mod cache;
pub mod local;
pub mod s3;

pub use cache::*;

/// An object that can be used to get and put blobs.
#[async_trait::async_trait]
pub trait BlobStorageProvider {
    async fn maybe_get(&self, blob_name: &str) -> Result<Option<Vec<u8>>, std::io::Error>;
    async fn put(&self, blob_name: &str, contents: Vec<u8>) -> Result<(), std::io::Error>;
    async fn list(&self, prefix: &str) -> Result<Vec<String>, std::io::Error>;
    async fn delete(&self, blob_name: &str) -> Result<(), std::io::Error>;

    fn can_put(&self) -> bool;
}

/// * read from remote
/// * if not found and can't write to remote => read disk and write to disk
/// * if not found and can write to remote => fetch and write
pub async fn cached_call<F: futures::Future<Output = Result<Vec<u8>, std::io::Error>>>(
    blob_name: &str,
    fetch: F,
    client: &dyn BlobStorageProvider,
    action: crate::cache::CacheAction,
) -> Result<Vec<u8>, std::io::Error> {
    let Some(data) = client.maybe_get(blob_name).await? else {
        if !client.can_put() {
            return cached(&blob_name, fetch, &crate::local::LocalDisk, action).await;
        } else {
            return cached(&blob_name, fetch, client, action).await;
        };
    };
    Ok(data)
}
