use itertools::Itertools;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Key {
    location: String,
    organ_id: usize,
}

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.location, self.organ_id)
    }
}

pub struct Autarquicas2025;

impl crate::fetch::Source for Autarquicas2025 {
    fn name() -> &'static str {
        "raw/autarquicas2025"
    }

    type KeyType = Key;

    async fn keys() -> impl Iterator<Item = Self::KeyType> {
        (1..=3588)
            .map(|location| location.to_string())
            .cartesian_product(vec![4, 5, 6])
            .map(|(location, organ_id)| Key { location, organ_id })
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
            format!("https://www.autarquicas2025.mai.gov.pt/resultados/territorio-nacional?local={}&election=CM", key.location).parse().unwrap(),
        );
        headers.insert(
            reqwest::header::ACCEPT,
            "application/json, text/javascript, */*; q=0.01"
                .parse()
                .unwrap(),
        );
        headers.insert(
            reqwest::header::ORIGIN,
            "https://www.autarquicas2025.mai.gov.pt".parse().unwrap(),
        );

        reqwest::Client::new()
            .post("https://www.autarquicas2025.mai.gov.pt/service/api/Result/territory")
            .json(&serde_json::json!({
                "organId": key.organ_id,
                "territoryId": key.location,
                "electionId": 1,
            }))
            .headers(headers)
            .build()
            .unwrap()
    }
}
