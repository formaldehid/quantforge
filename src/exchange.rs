use crate::{
    AccountTrade, AssetBalance, CancelOrderRequest, Candle, ExchangeError, ExchangeId,
    ExchangeOrder, Fill, KlineRequest, MarketDataSource, MarketOrderRequest, OrderQueryRequest,
    OrderStatus, Side, Symbol, SymbolRules, TradingVenue, now_utc_ms,
};
use async_trait::async_trait;
use hmac::{Hmac, Mac};
use reqwest::{Method, StatusCode};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;
use std::env;
use url::{Url, form_urlencoded};

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Debug)]
pub struct BinanceCredentials {
    pub api_key: String,
    pub secret: String,
}

impl BinanceCredentials {
    pub fn from_env() -> Option<Self> {
        let api_key = env::var("QF_BINANCE_API_KEY").ok()?;
        let secret = env::var("QF_BINANCE_API_SECRET").ok()?;
        Some(Self { api_key, secret })
    }

    pub fn from_required_env() -> Result<Self, ExchangeError> {
        match Self::from_env() {
            Some(value) => Ok(value),
            None => Err(ExchangeError::MissingCredentials),
        }
    }
}

#[derive(Clone, Debug)]
pub struct BinanceSpotClient {
    base_url: Url,
    http: reqwest::Client,
    credentials: Option<BinanceCredentials>,
    recv_window_ms: u64,
}

impl BinanceSpotClient {
    pub fn new(mut base_url: Url) -> Self {
        if !base_url.as_str().ends_with('/') {
            base_url.set_path(&format!("{}/", base_url.path()));
        }
        Self {
            base_url,
            http: reqwest::Client::new(),
            credentials: None,
            recv_window_ms: 5_000,
        }
    }

    pub fn with_credentials(mut self, credentials: BinanceCredentials) -> Self {
        self.credentials = Some(credentials);
        self
    }

    pub fn with_http(mut self, http: reqwest::Client) -> Self {
        self.http = http;
        self
    }

    pub fn with_recv_window_ms(mut self, recv_window_ms: u64) -> Self {
        self.recv_window_ms = recv_window_ms;
        self
    }

    fn join(&self, path: &str) -> Result<Url, ExchangeError> {
        self.base_url
            .join(path)
            .map_err(|err| ExchangeError::InvalidResponse {
                message: format!("failed to join base URL and path `{path}`: {err}"),
            })
    }

    fn signed_query(&self, params: Vec<(&str, String)>) -> Result<String, ExchangeError> {
        let credentials = self
            .credentials
            .as_ref()
            .ok_or(ExchangeError::MissingCredentials)?;

        let mut serializer = form_urlencoded::Serializer::new(String::new());
        for (key, value) in params {
            serializer.append_pair(key, &value);
        }
        serializer.append_pair("recvWindow", &self.recv_window_ms.to_string());
        serializer.append_pair("timestamp", &now_utc_ms().to_string());
        let query = serializer.finish();

        let mut mac = HmacSha256::new_from_slice(credentials.secret.as_bytes()).map_err(|err| {
            ExchangeError::InvalidRequest {
                message: format!("invalid HMAC secret: {err}"),
            }
        })?;
        mac.update(query.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        Ok(format!("{query}&signature={signature}"))
    }

    async fn send_public(
        &self,
        method: Method,
        path: &str,
        params: Vec<(&str, String)>,
    ) -> Result<Value, ExchangeError> {
        let mut url = self.join(path)?;
        let query = encode_query(params);
        if !query.is_empty() {
            url.set_query(Some(&query));
        }

        let response = self
            .http
            .request(method, url)
            .send()
            .await
            .map_err(ExchangeError::transport)?;

        decode_json(response).await
    }

    async fn send_signed(
        &self,
        method: Method,
        path: &str,
        params: Vec<(&str, String)>,
    ) -> Result<Value, ExchangeError> {
        let credentials = self
            .credentials
            .as_ref()
            .ok_or(ExchangeError::MissingCredentials)?;

        let mut url = self.join(path)?;
        let query = self.signed_query(params)?;
        url.set_query(Some(&query));

        let response = self
            .http
            .request(method, url)
            .header("X-MBX-APIKEY", &credentials.api_key)
            .send()
            .await
            .map_err(ExchangeError::transport)?;

        decode_json(response).await
    }
}

#[async_trait]
impl MarketDataSource for BinanceSpotClient {
    fn exchange_id(&self) -> ExchangeId {
        ExchangeId::BinanceSpot
    }

    async fn fetch_klines(&self, request: &KlineRequest) -> Result<Vec<Candle>, ExchangeError> {
        let mut params = vec![
            ("symbol", request.symbol.as_str().to_string()),
            ("interval", request.interval.as_str().to_string()),
        ];
        if let Some(start_time_ms) = request.start_time_ms {
            params.push(("startTime", start_time_ms.to_string()));
        }
        if let Some(end_time_ms) = request.end_time_ms {
            params.push(("endTime", end_time_ms.to_string()));
        }
        if let Some(limit) = request.limit {
            params.push(("limit", limit.min(1000).to_string()));
        }

        let raw = self
            .send_public(Method::GET, "api/v3/klines", params)
            .await?;
        let rows: Vec<BinanceKlineRow> =
            serde_json::from_value(raw).map_err(|err| ExchangeError::InvalidResponse {
                message: format!("failed to decode klines response: {err}"),
            })?;

        let mut candles = Vec::with_capacity(rows.len());
        for row in rows {
            candles.push(Candle {
                open_time_ms: row.0,
                open: parse_decimal(&row.1, "open")?,
                high: parse_decimal(&row.2, "high")?,
                low: parse_decimal(&row.3, "low")?,
                close: parse_decimal(&row.4, "close")?,
                volume: parse_decimal(&row.5, "volume")?,
                close_time_ms: row.6,
                trades: Some(row.8),
            });
        }

        Ok(candles)
    }

    async fn fetch_symbol_rules(&self, symbol: &Symbol) -> Result<SymbolRules, ExchangeError> {
        let raw = self
            .send_public(
                Method::GET,
                "api/v3/exchangeInfo",
                vec![("symbol", symbol.as_str().to_string())],
            )
            .await?;

        let response: BinanceExchangeInfoResponse =
            serde_json::from_value(raw).map_err(|err| ExchangeError::InvalidResponse {
                message: format!("failed to decode exchangeInfo response: {err}"),
            })?;

        let info =
            response
                .symbols
                .into_iter()
                .next()
                .ok_or_else(|| ExchangeError::InvalidResponse {
                    message: format!(
                        "exchangeInfo returned no symbol rules for {}",
                        symbol.as_str()
                    ),
                })?;

        Ok(parse_symbol_rules(info)?)
    }
}

#[async_trait]
impl TradingVenue for BinanceSpotClient {
    fn exchange_id(&self) -> ExchangeId {
        ExchangeId::BinanceSpot
    }

    async fn account_balances(&self) -> Result<Vec<AssetBalance>, ExchangeError> {
        let raw = self
            .send_signed(
                Method::GET,
                "api/v3/account",
                vec![("omitZeroBalances", "true".to_string())],
            )
            .await?;

        let response: BinanceAccountResponse =
            serde_json::from_value(raw).map_err(|err| ExchangeError::InvalidResponse {
                message: format!("failed to decode account response: {err}"),
            })?;

        let mut balances = Vec::with_capacity(response.balances.len());
        for balance in response.balances {
            balances.push(AssetBalance {
                asset: balance.asset,
                free: parse_decimal(&balance.free, "free")?,
                locked: parse_decimal(&balance.locked, "locked")?,
            });
        }
        Ok(balances)
    }

    async fn open_orders(
        &self,
        symbol: Option<&Symbol>,
    ) -> Result<Vec<ExchangeOrder>, ExchangeError> {
        let mut params = Vec::new();
        if let Some(symbol) = symbol {
            params.push(("symbol", symbol.as_str().to_string()));
        }

        let raw = self
            .send_signed(Method::GET, "api/v3/openOrders", params)
            .await?;
        let raw_items: Vec<Value> =
            serde_json::from_value(raw).map_err(|err| ExchangeError::InvalidResponse {
                message: format!("failed to decode openOrders response: {err}"),
            })?;

        raw_items.into_iter().map(parse_order).collect()
    }

    async fn recent_trades(
        &self,
        symbol: &Symbol,
        limit: usize,
    ) -> Result<Vec<AccountTrade>, ExchangeError> {
        let raw = self
            .send_signed(
                Method::GET,
                "api/v3/myTrades",
                vec![
                    ("symbol", symbol.as_str().to_string()),
                    ("limit", limit.min(1000).to_string()),
                ],
            )
            .await?;

        let rows: Vec<BinanceAccountTrade> =
            serde_json::from_value(raw).map_err(|err| ExchangeError::InvalidResponse {
                message: format!("failed to decode myTrades response: {err}"),
            })?;

        let mut trades = Vec::with_capacity(rows.len());
        for row in rows {
            trades.push(AccountTrade {
                symbol: Symbol::new(row.symbol)?,
                trade_id: row.id,
                order_id: row.order_id,
                side: if row.is_buyer { Side::Buy } else { Side::Sell },
                price: parse_decimal(&row.price, "trade.price")?,
                qty: parse_decimal(&row.qty, "trade.qty")?,
                quote_qty: parse_decimal(&row.quote_qty, "trade.quoteQty")?,
                commission: parse_decimal(&row.commission, "trade.commission")?,
                commission_asset: Some(row.commission_asset),
                time_ms: row.time,
            });
        }

        Ok(trades)
    }

    async fn submit_market_order(
        &self,
        request: &MarketOrderRequest,
    ) -> Result<ExchangeOrder, ExchangeError> {
        if request.quantity.is_none() && request.quote_order_qty.is_none() {
            return Err(ExchangeError::InvalidRequest {
                message: "submit_market_order requires quantity or quote_order_qty".to_string(),
            });
        }

        let mut params = vec![
            ("symbol", request.symbol.as_str().to_string()),
            ("side", request.side.as_str().to_string()),
            ("type", "MARKET".to_string()),
            ("newOrderRespType", "FULL".to_string()),
        ];

        if let Some(quantity) = request.quantity {
            params.push(("quantity", quantity.to_string()));
        }
        if let Some(quote_order_qty) = request.quote_order_qty {
            params.push(("quoteOrderQty", quote_order_qty.to_string()));
        }
        if let Some(client_order_id) = &request.new_client_order_id {
            params.push(("newClientOrderId", client_order_id.clone()));
        }

        let raw = self
            .send_signed(Method::POST, "api/v3/order", params)
            .await?;
        parse_order(raw)
    }

    async fn cancel_order(
        &self,
        request: &CancelOrderRequest,
    ) -> Result<ExchangeOrder, ExchangeError> {
        let mut params = vec![("symbol", request.symbol.as_str().to_string())];
        if let Some(order_id) = request.order_id {
            params.push(("orderId", order_id.to_string()));
        }
        if let Some(client_order_id) = &request.client_order_id {
            params.push(("origClientOrderId", client_order_id.clone()));
        }
        if request.order_id.is_none() && request.client_order_id.is_none() {
            return Err(ExchangeError::InvalidRequest {
                message: "cancel_order requires order_id or client_order_id".to_string(),
            });
        }

        let raw = self
            .send_signed(Method::DELETE, "api/v3/order", params)
            .await?;
        parse_order(raw)
    }

    async fn query_order(
        &self,
        request: &OrderQueryRequest,
    ) -> Result<ExchangeOrder, ExchangeError> {
        let mut params = vec![("symbol", request.symbol.as_str().to_string())];
        if let Some(order_id) = request.order_id {
            params.push(("orderId", order_id.to_string()));
        }
        if let Some(client_order_id) = &request.client_order_id {
            params.push(("origClientOrderId", client_order_id.clone()));
        }
        if request.order_id.is_none() && request.client_order_id.is_none() {
            return Err(ExchangeError::InvalidRequest {
                message: "query_order requires order_id or client_order_id".to_string(),
            });
        }

        let raw = self
            .send_signed(Method::GET, "api/v3/order", params)
            .await?;
        parse_order(raw)
    }
}

async fn decode_json(response: reqwest::Response) -> Result<Value, ExchangeError> {
    let status = response.status();
    let body = response.text().await.map_err(ExchangeError::transport)?;

    if !status.is_success() {
        if let Ok(api_error) = serde_json::from_str::<BinanceApiError>(&body) {
            return Err(ExchangeError::Api {
                code: Some(api_error.code),
                message: api_error.msg,
            });
        }

        return Err(ExchangeError::Api {
            code: status_to_code(status),
            message: format!("http {status}: {body}"),
        });
    }

    serde_json::from_str::<Value>(&body).map_err(|err| ExchangeError::InvalidResponse {
        message: format!("failed to decode JSON body: {err}; body={body}"),
    })
}

fn status_to_code(status: StatusCode) -> Option<i64> {
    Some(i64::from(status.as_u16()))
}

fn encode_query(params: Vec<(&str, String)>) -> String {
    let mut serializer = form_urlencoded::Serializer::new(String::new());
    for (key, value) in params {
        serializer.append_pair(key, &value);
    }
    serializer.finish()
}

fn parse_symbol_rules(info: BinanceSymbolInfo) -> Result<SymbolRules, ExchangeError> {
    let mut rules = SymbolRules {
        symbol: Symbol::new(info.symbol)?,
        base_asset: info.base_asset,
        quote_asset: info.quote_asset,
        min_qty: None,
        max_qty: None,
        step_size: None,
        market_min_qty: None,
        market_max_qty: None,
        market_step_size: None,
        min_notional: None,
        tick_size: None,
    };

    for filter in info.filters {
        let filter_type = filter
            .get("filterType")
            .and_then(Value::as_str)
            .unwrap_or_default();

        match filter_type {
            "LOT_SIZE" => {
                rules.min_qty = parse_optional_filter_decimal(&filter, "minQty")?;
                rules.max_qty = parse_optional_filter_decimal(&filter, "maxQty")?;
                rules.step_size = parse_optional_filter_decimal(&filter, "stepSize")?;
            }
            "MARKET_LOT_SIZE" => {
                rules.market_min_qty = parse_optional_filter_decimal(&filter, "minQty")?;
                rules.market_max_qty = parse_optional_filter_decimal(&filter, "maxQty")?;
                rules.market_step_size = parse_optional_filter_decimal(&filter, "stepSize")?;
            }
            "MIN_NOTIONAL" => {
                rules.min_notional = parse_optional_filter_decimal(&filter, "minNotional")?;
            }
            "NOTIONAL" => {
                if rules.min_notional.is_none() {
                    rules.min_notional = parse_optional_filter_decimal(&filter, "minNotional")?;
                }
            }
            "PRICE_FILTER" => {
                rules.tick_size = parse_optional_filter_decimal(&filter, "tickSize")?;
            }
            _ => {}
        }
    }

    Ok(rules)
}

fn parse_optional_filter_decimal(
    filter: &Value,
    field: &str,
) -> Result<Option<Decimal>, ExchangeError> {
    match filter.get(field).and_then(Value::as_str) {
        Some(raw) => Ok(Some(parse_decimal(raw, field)?)),
        None => Ok(None),
    }
}

fn parse_order(raw: Value) -> Result<ExchangeOrder, ExchangeError> {
    let response: BinanceOrderResponse =
        serde_json::from_value(raw.clone()).map_err(|err| ExchangeError::InvalidResponse {
            message: format!("failed to decode order response: {err}; raw={raw}"),
        })?;

    let symbol = Symbol::new(response.symbol)?;
    let side = response.side.parse::<Side>()?;
    let fills = response
        .fills
        .into_iter()
        .map(|fill| {
            Ok(Fill {
                price: parse_decimal(&fill.price, "fill.price")?,
                qty: parse_decimal(&fill.qty, "fill.qty")?,
                commission: parse_decimal(&fill.commission, "fill.commission")?,
                commission_asset: fill.commission_asset,
                trade_id: fill.trade_id,
            })
        })
        .collect::<Result<Vec<_>, ExchangeError>>()?;

    let requested_qty = match response.orig_qty {
        Some(value) => Some(parse_decimal(&value, "origQty")?),
        None => None,
    };
    let requested_quote_qty = match response.orig_quote_order_qty {
        Some(value) => {
            let parsed = parse_decimal(&value, "origQuoteOrderQty")?;
            if parsed > Decimal::ZERO {
                Some(parsed)
            } else {
                None
            }
        }
        None => None,
    };

    let executed_qty = parse_decimal(&response.executed_qty, "executedQty")?;
    let cumulative_quote_qty = parse_decimal(&response.cumulative_quote_qty, "cumulativeQuoteQty")?;
    let avg_price = if executed_qty > Decimal::ZERO {
        Some(cumulative_quote_qty / executed_qty)
    } else {
        None
    };

    Ok(ExchangeOrder {
        symbol,
        side,
        order_type: response.order_type,
        status: OrderStatus::from_exchange(response.status.as_deref().unwrap_or("UNKNOWN")),
        order_id: response.order_id,
        client_order_id: response.client_order_id,
        requested_qty,
        requested_quote_qty,
        executed_qty,
        cumulative_quote_qty,
        avg_price,
        transact_time_ms: response.transact_time,
        fills,
        raw,
    })
}

fn parse_decimal(raw: &str, field: &str) -> Result<Decimal, ExchangeError> {
    raw.parse::<Decimal>()
        .map_err(|err| ExchangeError::InvalidResponse {
            message: format!("failed to parse decimal field `{field}`: {err}"),
        })
}

#[derive(Debug, Deserialize)]
struct BinanceApiError {
    code: i64,
    msg: String,
}

#[derive(Debug, Deserialize)]
struct BinanceExchangeInfoResponse {
    symbols: Vec<BinanceSymbolInfo>,
}

#[derive(Debug, Deserialize)]
struct BinanceSymbolInfo {
    symbol: String,
    #[serde(rename = "baseAsset")]
    base_asset: String,
    #[serde(rename = "quoteAsset")]
    quote_asset: String,
    filters: Vec<Value>,
}

#[derive(Debug, Deserialize)]
struct BinanceAccountResponse {
    balances: Vec<BinanceBalance>,
}

#[derive(Debug, Deserialize)]
struct BinanceBalance {
    asset: String,
    free: String,
    locked: String,
}

#[derive(Debug, Deserialize)]
struct BinanceAccountTrade {
    symbol: String,
    id: i64,
    #[serde(rename = "orderId")]
    order_id: i64,
    price: String,
    qty: String,
    #[serde(rename = "quoteQty")]
    quote_qty: String,
    commission: String,
    #[serde(rename = "commissionAsset")]
    commission_asset: String,
    time: i64,
    #[serde(rename = "isBuyer")]
    is_buyer: bool,
}

#[derive(Debug, Deserialize)]
struct BinanceFill {
    price: String,
    qty: String,
    commission: String,
    #[serde(rename = "commissionAsset")]
    commission_asset: Option<String>,
    #[serde(rename = "tradeId")]
    trade_id: Option<i64>,
}

fn default_zero_string() -> String {
    "0".to_string()
}

#[derive(Debug, Deserialize)]
struct BinanceOrderResponse {
    symbol: String,
    #[serde(rename = "orderId")]
    order_id: Option<i64>,
    #[serde(rename = "clientOrderId")]
    client_order_id: Option<String>,
    side: String,
    #[serde(rename = "type")]
    order_type: String,
    status: Option<String>,
    #[serde(rename = "origQty")]
    orig_qty: Option<String>,
    #[serde(rename = "origQuoteOrderQty")]
    orig_quote_order_qty: Option<String>,
    #[serde(rename = "executedQty", default = "default_zero_string")]
    executed_qty: String,
    #[serde(
        rename = "cummulativeQuoteQty",
        alias = "cumulativeQuoteQty",
        default = "default_zero_string"
    )]
    cumulative_quote_qty: String,
    #[serde(rename = "transactTime")]
    transact_time: Option<i64>,
    #[serde(default)]
    fills: Vec<BinanceFill>,
}

#[derive(Debug, Deserialize, Serialize)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_query_works() {
        assert_eq!(
            encode_query(vec![
                ("symbol", "BTCUSDT".to_string()),
                ("limit", "10".to_string())
            ]),
            "symbol=BTCUSDT&limit=10"
        );
    }

    #[test]
    fn parse_order_maps_side_and_status() {
        let raw = serde_json::json!({
            "symbol": "BTCUSDT",
            "orderId": 7,
            "clientOrderId": "abc",
            "side": "BUY",
            "type": "MARKET",
            "status": "FILLED",
            "origQty": "0.01000000",
            "executedQty": "0.01000000",
            "cummulativeQuoteQty": "100.00000000",
            "transactTime": 1,
            "fills": []
        });

        let order = parse_order(raw).expect("order");
        assert_eq!(order.side, Side::Buy);
        assert_eq!(order.status, OrderStatus::Filled);
        assert_eq!(order.order_id, Some(7));
    }
}
