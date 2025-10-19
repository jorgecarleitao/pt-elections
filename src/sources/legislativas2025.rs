use std::vec;

use futures::StreamExt;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{RetryTransientMiddleware, policies::ExponentialBackoff};

#[derive(Debug, Clone, serde::Deserialize)]
pub struct Territory {
    #[serde(rename = "territoryKey")]
    territory_key: String,
}

fn client() -> ClientWithMiddleware {
    ClientBuilder::new(reqwest::Client::new())
        .with(RetryTransientMiddleware::new_with_policy(
            ExponentialBackoff::builder().build_with_max_retries(5),
        ))
        .build()
}

async fn fetch_many(territories: Vec<Territory>) -> Vec<Territory> {
    let tasks = territories.iter().map(|t| {
        let key = t.territory_key.clone();
        async move {
            let mut children = fetch_territories(&key).await;
            let mut all = vec![t.clone()];
            all.append(&mut children);
            all
        }
    });
    let children = futures::stream::iter(tasks)
        // this is recursive and can quickly run out of control => 4
        .buffered(4)
        .collect::<Vec<_>>()
        .await;

    territories
        .into_iter()
        .chain(children.into_iter().flatten())
        .collect()
}

#[tracing::instrument]
async fn fetch_territories(territory: &str) -> Vec<Territory> {
    tracing::info!("Fetching territories for {}", territory);
    if !territory.ends_with("0") {
        // no need to fetch territories for non-leaf nodes
        return vec![];
    }

    let url = format!(
        "https://www.eleicoes.mai.gov.pt/legislativas2025/assets/static/territory-children/territory-children-{}.json",
        territory
    );

    let response = client().get(url).send().await.unwrap();
    let territories = if response.status() == reqwest::StatusCode::NOT_FOUND {
        vec![]
    } else if response.status() == reqwest::StatusCode::OK {
        response
            .json::<Vec<Territory>>()
            .await
            .unwrap_or_else(|err| {
                tracing::error!(
                    territory = territory,
                    error = %err,
                    "Failed to parse territories"
                );
                vec![]
            })
    } else {
        tracing::error!(
            territory = territory,
            status = %response.status(),
            "Failed to fetch territories"
        );
        vec![]
    };

    fetch_many(territories).await
}

pub struct Legislativas2025;

impl crate::fetch::Source for Legislativas2025 {
    fn name() -> &'static str {
        "raw/legislativas2025"
    }

    type KeyType = String;

    async fn keys() -> impl Iterator<Item = Self::KeyType> {
        let territories = vec![
            Territory {
                territory_key: "LOCAL-500000".to_string(),
            },
            Territory {
                territory_key: "FOREIGN-600000".to_string(),
            },
        ];
        fetch_many(territories)
            .await
            .into_iter()
            .map(|t| t.territory_key)
    }

    fn request(key: &Self::KeyType) -> reqwest::Request {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::USER_AGENT,
            reqwest::header::HeaderValue::from_str(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:144.0) Gecko/20100101 Firefox/144.0",
            )
            .unwrap(),
        );

        headers.insert(
            reqwest::header::REFERER,
            format!("https://www.eleicoes.mai.gov.pt/legislativas2025/resultados/territorio-nacional?local={}", key)
                .parse()
                .unwrap(),
        );

        client()
            .get(format!("https://www.eleicoes.mai.gov.pt/legislativas2025/assets/static/territory-results/territory-results-{}-AR.json", key))
            .headers(headers)
            .build()
            .unwrap()
    }
}
