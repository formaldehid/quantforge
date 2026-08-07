//! Scriptable mock Binance Spot server for fully offline e2e tests.
//!
//! [`MockBinance`] is a hand-written HTTP/1.1 server (hyper, bound to an
//! ephemeral `127.0.0.1` port) implementing the Binance Spot surface the
//! CLI touches:
//!
//! - `GET  /api/v3/klines` and `GET /api/v3/exchangeInfo` (public)
//! - `GET  /api/v3/account`, `GET /api/v3/openOrders`, `GET /api/v3/myTrades`
//! - `POST /api/v3/order`, `DELETE /api/v3/order`, `GET /api/v3/order`
//!
//! Signed endpoints accept any `signature`/`timestamp`/`recvWindow`, so
//! tests run with fake credentials and zero network.
//!
//! # Scripting a scenario
//!
//! Responses are scripted per endpoint as FIFO queues via
//! [`Scenario::enqueue`] (or the typed conveniences that wrap it): each
//! request to an endpoint pops the next canned response. When a queue runs
//! dry, `klines` falls back to `[]` (the sync loop's termination signal)
//! and every other endpoint answers HTTP 500 with an "unscripted" marker so
//! a test that under-scripts fails loudly instead of hanging.
//!
//! Error scenarios are plain responses: [`CannedResponse::api_error`]
//! produces a Binance `{code, msg}` body with the HTTP status of your
//! choice (400/418/429/5xx), and [`CannedResponse::body`] serves arbitrary
//! payloads (malformed JSON, wrong shapes, ACK-instead-of-FULL orders).
//!
//! Every request the server sees is recorded and available through
//! [`MockBinance::requests`] for asserting methods, paths, query
//! parameters, and the `X-MBX-APIKEY` header.
//!
//! Fixture builders at the bottom mirror payloads recorded from the real
//! Binance Spot testnet (`testnet.binance.vision`, 2026-08-07) and from the
//! wire fixtures embedded in `src/exchange.rs`'s parser tests: 12-element
//! kline rows, the `exchangeInfo` filter list (including its numeric-valued
//! filters the client must skip), FULL / ACK / query-shaped order
//! responses, and `account` balances.
//!
//! ```text
//! let mut scenario = Scenario::new();
//! scenario.exchange_info(exchange_info_btcusdt());
//! scenario.klines_page(vec![kline_row(open_ms, "100.00000000")]);
//! scenario.order_response(order_full_response(
//!     "BUY", 1_001, "0.01000000", "100.00000000", "10000.00000000", "0.00001000",
//! ));
//! let mock = MockBinance::start(scenario);
//! // point the CLI at mock.base_url() via --binance-base-url
//! ```

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

/// One canned HTTP response, served once in FIFO order.
#[derive(Clone, Debug)]
pub struct CannedResponse {
    pub status: u16,
    pub body: String,
}

impl CannedResponse {
    /// A 200 response with a JSON payload.
    pub fn json(value: Value) -> Self {
        Self {
            status: 200,
            body: value.to_string(),
        }
    }

    /// A Binance-style error: `{code, msg}` body with the given HTTP status
    /// (Binance uses 4xx/5xx plus the 418/429 rate-limit statuses).
    pub fn api_error(status: u16, code: i64, msg: &str) -> Self {
        Self {
            status,
            body: json!({ "code": code, "msg": msg }).to_string(),
        }
    }

    /// An arbitrary body with the given status — for malformed JSON,
    /// wrong-shape payloads, or plain-text rate-limit responses.
    pub fn body(status: u16, body: &str) -> Self {
        Self {
            status,
            body: body.to_string(),
        }
    }
}

/// A request the mock served, recorded for assertions.
#[derive(Clone, Debug)]
pub struct RecordedRequest {
    pub method: String,
    pub path: String,
    pub query: String,
    pub api_key: Option<String>,
}

impl RecordedRequest {
    /// True when the query string contains `name=value` as a whole
    /// parameter (bounded by `&` or the string end).
    pub fn has_param(&self, name: &str, value: &str) -> bool {
        self.query
            .split('&')
            .any(|pair| pair == format!("{name}={value}"))
    }

    /// True when the query string carries the parameter at all.
    pub fn has_param_named(&self, name: &str) -> bool {
        self.query
            .split('&')
            .any(|pair| pair.split('=').next() == Some(name))
    }
}

#[derive(Debug, Default)]
struct ScenarioState {
    queues: HashMap<(String, String), VecDeque<CannedResponse>>,
    requests: Vec<RecordedRequest>,
}

/// Per-test script: FIFO response queues keyed by (method, path).
#[derive(Debug, Default)]
pub struct Scenario {
    state: ScenarioState,
}

impl Scenario {
    pub fn new() -> Self {
        Self::default()
    }

    /// Core scripting primitive: queue `response` for the next request of
    /// `method` on `path` (e.g. `("GET", "/api/v3/klines")`).
    pub fn enqueue(&mut self, method: &str, path: &str, response: CannedResponse) {
        self.state
            .queues
            .entry((method.to_string(), path.to_string()))
            .or_default()
            .push_back(response);
    }

    /// Queue an `exchangeInfo` payload.
    pub fn exchange_info(&mut self, symbols_envelope: Value) {
        self.enqueue(
            "GET",
            "/api/v3/exchangeInfo",
            CannedResponse::json(symbols_envelope),
        );
    }

    /// Queue one klines page (a JSON array of 12-element rows). A drained
    /// queue serves `[]`, which ends the client's sync loop.
    pub fn klines_page(&mut self, rows: Vec<Value>) {
        self.enqueue("GET", "/api/v3/klines", CannedResponse::json(json!(rows)));
    }

    /// Queue an `account` payload.
    pub fn account(&mut self, balances_envelope: Value) {
        self.enqueue(
            "GET",
            "/api/v3/account",
            CannedResponse::json(balances_envelope),
        );
    }

    /// Queue an `openOrders` payload.
    pub fn open_orders(&mut self, orders: Value) {
        self.enqueue("GET", "/api/v3/openOrders", CannedResponse::json(orders));
    }

    /// Queue a `myTrades` payload.
    pub fn my_trades(&mut self, trades: Value) {
        self.enqueue("GET", "/api/v3/myTrades", CannedResponse::json(trades));
    }

    /// Queue a response for the next `POST /api/v3/order`.
    pub fn order_response(&mut self, order: Value) {
        self.enqueue("POST", "/api/v3/order", CannedResponse::json(order));
    }

    /// Queue a response for the next `DELETE /api/v3/order`.
    pub fn order_cancel_response(&mut self, order: Value) {
        self.enqueue("DELETE", "/api/v3/order", CannedResponse::json(order));
    }

    /// Queue a response for the next `GET /api/v3/order`.
    pub fn order_query_response(&mut self, order: Value) {
        self.enqueue("GET", "/api/v3/order", CannedResponse::json(order));
    }
}

fn respond(
    state: &Mutex<ScenarioState>,
    request: &Request<hyper::body::Incoming>,
) -> (u16, String) {
    let method = request.method().to_string();
    let path = request.uri().path().to_string();
    let query = request.uri().query().unwrap_or_default().to_string();
    let api_key = request
        .headers()
        .get("X-MBX-APIKEY")
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_string());

    let mut state = state.lock().expect("lock");
    state.requests.push(RecordedRequest {
        method: method.clone(),
        path: path.clone(),
        query,
        api_key,
    });

    if let Some(canned) = state
        .queues
        .get_mut(&(method.clone(), path.clone()))
        .and_then(|queue| queue.pop_front())
    {
        return (canned.status, canned.body);
    }

    // Defaults for drained queues: klines terminates the sync loop with an
    // empty page; everything else fails loudly so an under-scripted test
    // surfaces immediately instead of hanging or passing by accident.
    if method == "GET" && path == "/api/v3/klines" {
        (200, "[]".to_string())
    } else {
        (
            500,
            json!({
                "code": -1000,
                "msg": format!("unscripted mock endpoint: {method} {path}")
            })
            .to_string(),
        )
    }
}

/// The running mock server. Shuts down when dropped.
#[derive(Debug)]
pub struct MockBinance {
    addr: SocketAddr,
    state: Arc<Mutex<ScenarioState>>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl MockBinance {
    /// Bind an ephemeral localhost port and serve `scenario` from a
    /// background thread with its own single-threaded runtime, so tests
    /// stay plain `#[test]` fns driving the CLI as a subprocess.
    pub fn start(scenario: Scenario) -> Self {
        let state = Arc::new(Mutex::new(scenario.state));
        let server_state = Arc::clone(&state);
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let (addr_tx, addr_rx) = std::sync::mpsc::channel::<SocketAddr>();

        let join = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            runtime.block_on(async move {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .expect("bind");
                addr_tx
                    .send(listener.local_addr().expect("local addr"))
                    .expect("send addr");

                loop {
                    tokio::select! {
                        _ = &mut shutdown_rx => break,
                        accepted = listener.accept() => {
                            let Ok((stream, _)) = accepted else { break };
                            let connection_state = Arc::clone(&server_state);
                            tokio::spawn(async move {
                                let service = service_fn(move |request| {
                                    let (status, body) = respond(&connection_state, &request);
                                    async move {
                                        Ok::<_, Infallible>(
                                            Response::builder()
                                                .status(
                                                    StatusCode::from_u16(status)
                                                        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                                                )
                                                .header("Content-Type", "application/json")
                                                .body(Full::new(Bytes::from(body)))
                                                .expect("response"),
                                        )
                                    }
                                });
                                let _ = hyper::server::conn::http1::Builder::new()
                                    .serve_connection(TokioIo::new(stream), service)
                                    .await;
                            });
                        }
                    }
                }
            });
        });

        let addr = addr_rx.recv().expect("mock addr");
        Self {
            addr,
            state,
            shutdown: Some(shutdown_tx),
            join: Some(join),
        }
    }

    /// Base URL for `--binance-base-url` (trailing slash included).
    pub fn base_url(&self) -> String {
        format!("http://{}/", self.addr)
    }

    /// Snapshot of every request served so far, in arrival order.
    pub fn requests(&self) -> Vec<RecordedRequest> {
        self.state.lock().expect("lock").requests.clone()
    }
}

impl Drop for MockBinance {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

// ── Fixture builders (shapes recorded from testnet.binance.vision and the
// ── wire fixtures in src/exchange.rs parser tests) ─────────────────────

/// One 12-element kline row as served by `GET /api/v3/klines`, mirroring a
/// row recorded from the Spot testnet. Open/high/low are derived from
/// `close` so signal scripting only needs the close; `close_time` is
/// `open + 59_999` (1m grid).
pub fn kline_row(open_time_ms: i64, close: &str) -> Value {
    json!([
        open_time_ms,
        close,
        close,
        close,
        close,
        "0.05411000",
        open_time_ms + 59_999,
        "3481.98051460",
        46,
        "0.02657000",
        "1709.81295580",
        "0"
    ])
}

/// `exchangeInfo` envelope for BTCUSDT with the full filter list recorded
/// from the Spot testnet: string-valued filters the client reads
/// (`PRICE_FILTER`, `LOT_SIZE`, `NOTIONAL`), the numeric-valued filters it
/// must skip (`ICEBERG_PARTS`, `TRAILING_DELTA`, ...), and a
/// `MARKET_LOT_SIZE` with testnet's real zero step (rounding degrades to a
/// no-op on a zero step, per `round_down_to_step`).
pub fn exchange_info_btcusdt() -> Value {
    json!({
        "timezone": "UTC",
        "serverTime": 1_786_066_000_000_i64,
        "rateLimits": [],
        "exchangeFilters": [],
        "symbols": [{
            "symbol": "BTCUSDT",
            "status": "TRADING",
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
            "orderTypes": ["LIMIT", "MARKET"],
            "quoteOrderQtyMarketAllowed": true,
            "isSpotTradingAllowed": true,
            "filters": [
                {
                    "filterType": "PRICE_FILTER",
                    "minPrice": "0.01000000",
                    "maxPrice": "1000000.00000000",
                    "tickSize": "0.01000000"
                },
                {
                    "filterType": "LOT_SIZE",
                    "minQty": "0.00001000",
                    "maxQty": "9000.00000000",
                    "stepSize": "0.00001000"
                },
                { "filterType": "ICEBERG_PARTS", "limit": 100 },
                {
                    "filterType": "MARKET_LOT_SIZE",
                    "minQty": "0.00000000",
                    "maxQty": "141.67845966",
                    "stepSize": "0.00000000"
                },
                {
                    "filterType": "TRAILING_DELTA",
                    "minTrailingAboveDelta": 10,
                    "maxTrailingAboveDelta": 2000,
                    "minTrailingBelowDelta": 10,
                    "maxTrailingBelowDelta": 2000
                },
                {
                    "filterType": "PERCENT_PRICE_BY_SIDE",
                    "bidMultiplierUp": "2",
                    "bidMultiplierDown": "0.5",
                    "askMultiplierUp": "2",
                    "askMultiplierDown": "0.5",
                    "avgPriceMins": 5
                },
                {
                    "filterType": "NOTIONAL",
                    "minNotional": "5.00000000",
                    "applyMinToMarket": true,
                    "maxNotional": "9000000.00000000",
                    "applyMaxToMarket": false,
                    "avgPriceMins": 5
                },
                { "filterType": "MAX_NUM_ORDERS", "maxNumOrders": 200 },
                { "filterType": "MAX_NUM_ALGO_ORDERS", "maxNumAlgoOrders": 5 }
            ]
        }]
    })
}

/// A FULL market-order response (the shape `newOrderRespType=FULL`
/// returns): executed quantities, cumulative quote, and one fill with a
/// base-asset commission — mirroring the canonical fixture in
/// `src/exchange.rs`. `fill_price` = `cummulative_quote` / `executed_qty`
/// is the caller's responsibility to keep coherent.
pub fn order_full_response(
    side: &str,
    order_id: i64,
    executed_qty: &str,
    cummulative_quote: &str,
    fill_price: &str,
    commission: &str,
) -> Value {
    json!({
        "symbol": "BTCUSDT",
        "orderId": order_id,
        "orderListId": -1,
        "clientOrderId": format!("mock-{order_id}"),
        "transactTime": 1_786_066_000_000_i64,
        "price": "0.00000000",
        "origQty": executed_qty,
        "executedQty": executed_qty,
        "cummulativeQuoteQty": cummulative_quote,
        "status": "FILLED",
        "timeInForce": "GTC",
        "type": "MARKET",
        "side": side,
        "workingTime": 1_786_066_000_000_i64,
        "fills": [{
            "price": fill_price,
            "qty": executed_qty,
            "commission": commission,
            "commissionAsset": if side == "BUY" { "BTC" } else { "USDT" },
            "tradeId": order_id * 100
        }]
    })
}

/// An ACK-shaped order response: identifiers only, no status, no
/// quantities, no fills (mirrors the ACK fixture in `src/exchange.rs`).
/// The live engine must refuse to update position state from this.
pub fn order_ack_response(side: &str, order_id: i64) -> Value {
    json!({
        "symbol": "BTCUSDT",
        "orderId": order_id,
        "orderListId": -1,
        "clientOrderId": format!("mock-{order_id}"),
        "transactTime": 1_786_066_000_000_i64,
        "type": "MARKET",
        "side": side
    })
}

/// A query/cancel-shaped order response: status and quantities but no
/// `fills` array (mirrors the query fixture in `src/exchange.rs`) — the
/// "missing fill data" case.
pub fn order_query_response(side: &str, order_id: i64, executed_qty: &str) -> Value {
    json!({
        "symbol": "BTCUSDT",
        "orderId": order_id,
        "clientOrderId": format!("mock-{order_id}"),
        "price": "0.00000000",
        "origQty": executed_qty,
        "executedQty": executed_qty,
        "cummulativeQuoteQty": "100.00000000",
        "status": "FILLED",
        "type": "MARKET",
        "side": side,
        "time": 1_786_066_000_000_i64
    })
}

/// An `account` envelope with one non-zero balance.
pub fn account_response(asset: &str, free: &str) -> Value {
    json!({
        "makerCommission": 0,
        "takerCommission": 0,
        "canTrade": true,
        "balances": [
            { "asset": asset, "free": free, "locked": "0.00000000" },
            { "asset": "USDT", "free": "10000.00000000", "locked": "0.00000000" }
        ]
    })
}

/// One `myTrades` row (all fields required by the client's decoder).
pub fn my_trade_row(trade_id: i64, order_id: i64, is_buyer: bool) -> Value {
    json!({
        "symbol": "BTCUSDT",
        "id": trade_id,
        "orderId": order_id,
        "orderListId": -1,
        "price": "10000.00000000",
        "qty": "0.01000000",
        "quoteQty": "100.00000000",
        "commission": "0.00001000",
        "commissionAsset": "BTC",
        "time": 1_786_066_000_000_i64,
        "isBuyer": is_buyer,
        "isMaker": false,
        "isBestMatch": true
    })
}
