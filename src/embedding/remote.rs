use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct EmbedRequest<'a> {
    model: &'a str,
    input: [&'a str; 1],
}

#[derive(Deserialize)]
struct EmbedResponse {
    data: Vec<EmbedData>,
}

#[derive(Deserialize)]
struct EmbedData {
    embedding: Vec<f32>,
}

/// Calls an OpenAI-compatible `/v1/embeddings` endpoint and returns the
/// embedding for `text`. Compatible with Ollama, OpenAI, Cohere (v1 compat),
/// and any other OpenAI-spec embeddings API.
pub async fn embed(
    client: &reqwest::Client,
    url: &str,
    model: &str,
    text: &str,
) -> anyhow::Result<Vec<f32>> {
    let body = EmbedRequest {
        model,
        input: [text],
    };
    let resp: EmbedResponse = client
        .post(url)
        .json(&body)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("remote embed request failed: {e}"))?
        .error_for_status()
        .map_err(|e| anyhow::anyhow!("remote embed API error: {e}"))?
        .json()
        .await
        .map_err(|e| anyhow::anyhow!("remote embed response parse failed: {e}"))?;

    resp.data
        .into_iter()
        .next()
        .map(|d| d.embedding)
        .ok_or_else(|| anyhow::anyhow!("remote embed API returned empty data array"))
}
