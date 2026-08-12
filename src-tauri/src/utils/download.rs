use futures_util::StreamExt;
use log::{info, warn};
use reqwest::Client;
use std::path::Path;
use std::time::Duration;
use tokio::io::AsyncWriteExt;

/// TCP/HTTP connect timeout (before headers arrive).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
/// Fail if no data arrives for this long. Slow-but-steady transfers (e.g.
/// 30 KB/s) are fine; a stalled connection is not.
const IDLE_TIMEOUT: Duration = Duration::from_secs(45);
/// Maximum download attempts before giving up.
const MAX_ATTEMPTS: u32 = 3;
/// Backoff between attempts (length == MAX_ATTEMPTS - 1).
const BACKOFF: [Duration; 2] = [Duration::from_secs(1), Duration::from_secs(3)];

/// Download `url` to `dest` with bounded retries and an idle-progress timeout.
///
/// The timeout measures *progress*, not wall-clock time: the transfer only
/// fails once no data has arrived for `IDLE_TIMEOUT`. This lets slow networks
/// (which still deliver bytes) complete while stuck connections fail fast and
/// are retried. On failure the destination is truncated so a retry starts
/// from a clean slate.
pub async fn download_file(url: &str, dest: &Path) -> Result<(), String> {
    let client = Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .build()
        .map_err(|e| format!("Failed to build HTTP client: {}", e))?;

    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        match try_download(&client, url, dest).await {
            Ok(()) => {
                let size = std::fs::metadata(dest)
                    .map(|m| m.len())
                    .unwrap_or(0);
                info!("Downloaded {} ({} bytes)", url, size);
                return Ok(());
            }
            Err(e) if attempt < MAX_ATTEMPTS => {
                let delay = BACKOFF[(attempt - 1) as usize];
                warn!(
                    "Download attempt {}/{} failed ({}), retrying in {}s",
                    attempt,
                    MAX_ATTEMPTS,
                    e,
                    delay.as_secs()
                );
                tokio::time::sleep(delay).await;
            }
            Err(e) => {
                return Err(format!(
                    "Failed to download after {} attempts: {}",
                    MAX_ATTEMPTS, e
                ));
            }
        }
    }
}

async fn try_download(client: &Client, url: &str, dest: &Path) -> Result<(), String> {
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|e| format!("Request failed: {}", e))?;

    if !response.status().is_success() {
        return Err(format!("HTTP {}", response.status()));
    }

    let total = response.content_length();
    let mut stream = response.bytes_stream();
    let mut file = tokio::fs::File::create(dest)
        .await
        .map_err(|e| format!("Failed to create file {}: {}", dest.display(), e))?;

    let mut received: u64 = 0;
    let mut last_logged_pct: u8 = 0;

    while let Some(chunk) = tokio::time::timeout(IDLE_TIMEOUT, stream.next())
        .await
        .map_err(|_| {
            format!(
                "Connection stalled (no data for {}s)",
                IDLE_TIMEOUT.as_secs()
            )
        })?
    {
        let chunk = chunk.map_err(|e| format!("Failed to read response body: {}", e))?;
        received += chunk.len() as u64;
        file.write_all(&chunk)
            .await
            .map_err(|e| format!("Failed to write to {}: {}", dest.display(), e))?;

        if let Some(total) = total {
            if total > 0 {
                let pct = (received as f64 * 100.0 / total as f64) as u8;
                if pct >= last_logged_pct + 10 || pct == 100 {
                    info!("Download progress: {}% ({} / {} bytes)", pct, received, total);
                    last_logged_pct = pct;
                }
            }
        }
    }

    file.flush()
        .await
        .map_err(|e| format!("Failed to flush {}: {}", dest.display(), e))?;

    Ok(())
}
