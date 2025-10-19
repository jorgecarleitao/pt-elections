use futures::{FutureExt, StreamExt};
use tracing;

mod sources;
mod fetch;

use crate::fetch::Source;

#[tokio::main]
async fn main() -> Result<(), String> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    tracing::info!("Logger initialized");
    let access_key = std::env::var("AWS_ACCESS_KEY_ID")
        .map_err(|_| "AWS_ACCESS_KEY_ID environment variable not set".to_string())?;
    let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY")
        .map_err(|_| "AWS_SECRET_ACCESS_KEY environment variable not set".to_string())?;

    let client = fs::s3::client(
        "pt-elections".to_string(),
        "https://fsn1.your-objectstorage.com".to_string(),
        "fsn1",
        Some(fs::s3::Credentials {
            access_key,
            secret_access_key,
        }),
    )
    .await;
    let client = &client;

    /*
    let tasks = sources::Autarquicas2025::keys().await.map(|key| {
        async move {
            let _ = fetch::cached::<sources::Autarquicas2025>(&key, client).await?;
            Ok::<(), std::io::Error>(())
        }
        .boxed()
    });

    let tasks = sources::Legislativas2025::keys().await.map(|key| {
        async move {
            let _ = fetch::cached::<sources::Legislativas2025>(&key, client).await?;
            Ok::<(), std::io::Error>(())
        }
        .boxed()
    }).chain(tasks);
 */

    let tasks = sources::Legislativas2022::keys().await.map(|key| {
        async move {
            let _ = fetch::cached::<sources::Legislativas2022>(&key, client).await?;
            Ok::<(), std::io::Error>(())
        }
        .boxed()
    });

    let _ = futures::stream::iter(tasks)
        .buffered(10)
        .map(|r| {
            if let Err(e) = r {
                tracing::error!("{e}");
            }
        })
        .collect::<Vec<_>>()
        .await;
    tracing::info!("execution completed");

    Ok(())
}
