use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::error::Error as StdError;
use thiserror::Error;
use tracing::debug;
use url::Url;

use crate::model::{Candle, ExchangeId, Interval, Symbol, TimestampMs};

#[derive(Clone, Debug)]
pub struct KlineRequest {
    pub symbol: Symbol,
    pub interval: Interval,
    pub start_time_ms: Option<TimestampMs>,
    pub end_time_ms: Option<TimestampMs>,
    pub limit: Option<u16>,
}

#[derive(Error, Debug)]
pub enum ExchangeError {
    #[error("transport error")]
    Transport(#[source] Box<dyn StdError + Send + Sync>),

    #[error("api error: code={code:?} message={message}")]
    Api { code: Option<i64>, message: String },

    #[error("invalid response: {message}")]
    InvalidResponse { message: String },
}

impl ExchangeError {
    pub fn transport<E>(err: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::Transport(Box::new(err))
    }
}

#[async_trait]
pub trait MarketDataSource: Send + Sync {
    fn exchange_id(&self) -> ExchangeId;
    async fn fetch_klines(&self, req: &KlineRequest) -> Result<Vec<Candle>, ExchangeError>;
}

#[derive(Clone, Debug)]
pub struct BinanceSpotClient {
    base_url: Url,
    http: reqwest::Client,
}

impl BinanceSpotClient {
    pub fn new(mut base_url: Url) -> Self {
        if !base_url.as_str().ends_with('/') {
            base_url.set_path(&format!("{}/", base_url.path()));
        }
        Self {
            base_url,
            http: reqwest::Client::new(),
        }
    }

    pub fn with_http(mut base_url: Url, http: reqwest::Client) -> Self {
        if !base_url.as_str().ends_with('/') {
            base_url.set_path(&format!("{}/", base_url.path()));
        }
        Self { base_url, http }
    }
}

#[derive(Serialize)]
struct KlinesParams<'a> {
    symbol: &'a str,
    interval: &'a str,
    #[serde(rename = "startTime", skip_serializing_if = "Option::is_none")]
    start_time: Option<i64>,
    #[serde(rename = "endTime", skip_serializing_if = "Option::is_none")]
    end_time: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<u16>,
}

#[derive(Deserialize, Debug)]
struct BinanceApiError {
    code: i64,
    msg: String,
}

#[derive(Deserialize, Debug)]
struct BinanceKlineRow(
    i64,
    String,
    String,
    String,
    String,
    String,
    i64,
    String,
    u64,
    String,
    String,
    String,
);

#[async_trait]
impl MarketDataSource for BinanceSpotClient {
    fn exchange_id(&self) -> ExchangeId {
        ExchangeId::BinanceSpot
    }

    async fn fetch_klines(&self, req: &KlineRequest) -> Result<Vec<Candle>, ExchangeError> {
        let url =
            self.base_url
                .join("api/v3/klines")
                .map_err(|e| ExchangeError::InvalidResponse {
                    message: format!("invalid base url join: {e}"),
                })?;

        let params = KlinesParams {
            symbol: req.symbol.as_str(),
            interval: req.interval.as_str(),
            start_time: req.start_time_ms,
            end_time: req.end_time_ms,
            limit: req.limit.map(|value| value.min(1000)),
        };

        debug!(
            %url,
            symbol = params.symbol,
            interval = params.interval,
            start_time_ms = params.start_time,
            end_time_ms = params.end_time,
            limit = params.limit,
            "fetching klines"
        );

        let response = self
            .http
            .get(url)
            .query(&params)
            .send()
            .await
            .map_err(ExchangeError::transport)?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.map_err(ExchangeError::transport)?;
            if let Ok(api_error) = serde_json::from_str::<BinanceApiError>(&body) {
                return Err(ExchangeError::Api {
                    code: Some(api_error.code),
                    message: api_error.msg,
                });
            }
            return Err(ExchangeError::Api {
                code: None,
                message: format!("http {status}: {body}"),
            });
        }

        let rows: Vec<BinanceKlineRow> = response.json().await.map_err(ExchangeError::transport)?;
        let mut candles = Vec::with_capacity(rows.len());

        for BinanceKlineRow(
            open_time,
            open_s,
            high_s,
            low_s,
            close_s,
            volume_s,
            close_time,
            _quote_asset_volume,
            trades,
            _taker_buy_base_volume,
            _taker_buy_quote_volume,
            _ignore,
        ) in rows
        {
            let open = open_s
                .parse::<Decimal>()
                .map_err(|e| ExchangeError::InvalidResponse {
                    message: format!("failed to parse open decimal: {e}"),
                })?;
            let high = high_s
                .parse::<Decimal>()
                .map_err(|e| ExchangeError::InvalidResponse {
                    message: format!("failed to parse high decimal: {e}"),
                })?;
            let low = low_s
                .parse::<Decimal>()
                .map_err(|e| ExchangeError::InvalidResponse {
                    message: format!("failed to parse low decimal: {e}"),
                })?;
            let close = close_s
                .parse::<Decimal>()
                .map_err(|e| ExchangeError::InvalidResponse {
                    message: format!("failed to parse close decimal: {e}"),
                })?;
            let volume =
                volume_s
                    .parse::<Decimal>()
                    .map_err(|e| ExchangeError::InvalidResponse {
                        message: format!("failed to parse volume decimal: {e}"),
                    })?;

            candles.push(Candle {
                open_time_ms: open_time,
                close_time_ms: close_time,
                open,
                high,
                low,
                close,
                volume,
                trades: Some(trades),
            });
        }

        Ok(candles)
    }
}
