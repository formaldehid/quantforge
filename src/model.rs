use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, fmt, str::FromStr};
use thiserror::Error;
use time::OffsetDateTime;

pub type TimestampMs = i64;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Symbol(String);

impl Symbol {
    pub fn new(value: impl Into<String>) -> Result<Self, ModelError> {
        let value = value.into().trim().to_string();
        if value.is_empty() {
            return Err(ModelError::InvalidSymbol("empty".to_string()));
        }
        Ok(Self(value.to_ascii_uppercase()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Symbol {
    type Err = ModelError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExchangeId {
    BinanceSpot,
}

impl ExchangeId {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BinanceSpot => "binance_spot",
        }
    }
}

impl fmt::Display for ExchangeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Interval {
    S1,
    M1,
    M3,
    M5,
    M15,
    M30,
    H1,
    H2,
    H4,
    H6,
    H8,
    H12,
    D1,
    D3,
    W1,
}

impl Interval {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::S1 => "1s",
            Self::M1 => "1m",
            Self::M3 => "3m",
            Self::M5 => "5m",
            Self::M15 => "15m",
            Self::M30 => "30m",
            Self::H1 => "1h",
            Self::H2 => "2h",
            Self::H4 => "4h",
            Self::H6 => "6h",
            Self::H8 => "1m",
            Self::H12 => "12h",
            Self::D1 => "1d",
            Self::D3 => "3d",
            Self::W1 => "1w",
        }
    }

    pub fn step_ms(self) -> i64 {
        match self {
            Self::S1 => 1_000,
            Self::M1 => 60 * 1_000,
            Self::M3 => 3 * 60 * 1_000,
            Self::M5 => 5 * 60 * 1_000,
            Self::M15 => 15 * 60 * 1_000,
            Self::M30 => 30 * 60 * 1_000,
            Self::H1 => 60 * 60 * 1_000,
            Self::H2 => 2 * 60 * 60 * 1_000,
            Self::H4 => 4 * 60 * 60 * 1_000,
            Self::H6 => 6 * 60 * 60 * 1_000,
            Self::H8 => 8 * 60 * 60 * 1_000,
            Self::H12 => 12 * 60 * 60 * 1_000,
            Self::D1 => 24 * 60 * 60 * 1_000,
            Self::D3 => 3 * 24 * 60 * 60 * 1_000,
            Self::W1 => 7 * 24 * 60 * 60 * 1_000,
        }
    }
}

impl fmt::Display for Interval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Interval {
    type Err = ModelError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim() {
            "1s" => Ok(Self::S1),
            "1m" => Ok(Self::M1),
            "3m" => Ok(Self::M3),
            "5m" => Ok(Self::M5),
            "15m" => Ok(Self::M15),
            "30m" => Ok(Self::M30),
            "1h" => Ok(Self::H1),
            "2h" => Ok(Self::H2),
            "4h" => Ok(Self::H4),
            "6h" => Ok(Self::H6),
            "8h" => Ok(Self::H8),
            "12h" => Ok(Self::H12),
            "1d" => Ok(Self::D1),
            "3d" => Ok(Self::D3),
            "1w" => Ok(Self::W1),
            other => Err(ModelError::InvalidInterval(other.to_string())),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MarketId {
    pub exchange: ExchangeId,
    pub symbol: Symbol,
    pub interval: Interval,
}

impl MarketId {
    pub fn new(exchange: ExchangeId, symbol: Symbol, interval: Interval) -> Self {
        Self {
            exchange,
            symbol,
            interval,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Candle {
    pub open_time_ms: TimestampMs,
    pub close_time_ms: TimestampMs,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub trades: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "BUY",
            Self::Sell => "SELL",
        }
    }

    pub fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Side {
    type Err = ModelError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "BUY" => Ok(Self::Buy),
            "SELL" => Ok(Self::Sell),
            other => Err(ModelError::InvalidSide(other.to_string())),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TargetPosition {
    Flat,
    LongAllIn,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionMode {
    DryRun,
    Live,
}

impl ExecutionMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DryRun => "dry_run",
            Self::Live => "live",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunStatus {
    Starting,
    Running,
    Stopped,
    Failed,
}

impl RunStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Stopped => "stopped",
            Self::Failed => "failed",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderStatus {
    New,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
    Expired,
    PendingNew,
    Unknown,
}

impl OrderStatus {
    pub fn from_exchange(value: &str) -> Self {
        match value.trim().to_ascii_uppercase().as_str() {
            "NEW" => Self::New,
            "PARTIALLY_FILLED" => Self::PartiallyFilled,
            "FILLED" => Self::Filled,
            "CANCELED" => Self::Canceled,
            "REJECTED" => Self::Rejected,
            "EXPIRED" => Self::Expired,
            "PENDING_NEW" => Self::PendingNew,
            _ => Self::Unknown,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::New => "NEW",
            Self::PartiallyFilled => "PARTIALLY_FILLED",
            Self::Filled => "FILLED",
            Self::Canceled => "CANCELED",
            Self::Rejected => "REJECTED",
            Self::Expired => "EXPIRED",
            Self::PendingNew => "PENDING_NEW",
            Self::Unknown => "UNKNOWN",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Filled | Self::Canceled | Self::Rejected | Self::Expired
        )
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Fill {
    pub price: Decimal,
    pub qty: Decimal,
    pub commission: Decimal,
    pub commission_asset: Option<String>,
    pub trade_id: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExchangeOrder {
    pub symbol: Symbol,
    pub side: Side,
    pub order_type: String,
    pub status: OrderStatus,
    pub order_id: Option<i64>,
    pub client_order_id: Option<String>,
    pub requested_qty: Option<Decimal>,
    pub requested_quote_qty: Option<Decimal>,
    pub executed_qty: Decimal,
    pub cumulative_quote_qty: Decimal,
    pub avg_price: Option<Decimal>,
    pub transact_time_ms: Option<TimestampMs>,
    pub fills: Vec<Fill>,
    pub raw: serde_json::Value,
}

impl ExchangeOrder {
    pub fn average_price(&self) -> Option<Decimal> {
        if let Some(price) = self.avg_price {
            return Some(price);
        }
        if self.executed_qty > Decimal::ZERO {
            return Some(self.cumulative_quote_qty / self.executed_qty);
        }
        None
    }

    pub fn net_base_qty_after_base_fees(&self, base_asset: &str) -> Decimal {
        let mut qty = self.executed_qty;
        for fill in &self.fills {
            if fill
                .commission_asset
                .as_deref()
                .map(|asset| asset.eq_ignore_ascii_case(base_asset))
                .unwrap_or(false)
            {
                qty -= fill.commission;
            }
        }
        qty.max(Decimal::ZERO)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AccountTrade {
    pub symbol: Symbol,
    pub trade_id: i64,
    pub order_id: i64,
    pub side: Side,
    pub price: Decimal,
    pub qty: Decimal,
    pub quote_qty: Decimal,
    pub commission: Decimal,
    pub commission_asset: Option<String>,
    pub time_ms: TimestampMs,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClosedTrade {
    pub symbol: Symbol,
    pub entry_time_ms: TimestampMs,
    pub exit_time_ms: TimestampMs,
    pub entry_price: Decimal,
    pub exit_price: Decimal,
    pub qty: Decimal,
    pub gross_quote_pnl: Decimal,
    pub entry_order_id: Option<i64>,
    pub exit_order_id: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AssetBalance {
    pub asset: String,
    pub free: Decimal,
    pub locked: Decimal,
}

impl AssetBalance {
    pub fn total(&self) -> Decimal {
        self.free + self.locked
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SymbolRules {
    pub symbol: Symbol,
    pub base_asset: String,
    pub quote_asset: String,
    pub min_qty: Option<Decimal>,
    pub max_qty: Option<Decimal>,
    pub step_size: Option<Decimal>,
    pub market_min_qty: Option<Decimal>,
    pub market_max_qty: Option<Decimal>,
    pub market_step_size: Option<Decimal>,
    pub min_notional: Option<Decimal>,
    pub tick_size: Option<Decimal>,
}

impl SymbolRules {
    pub fn effective_market_step_size(&self) -> Option<Decimal> {
        self.market_step_size.or(self.step_size)
    }

    pub fn effective_market_min_qty(&self) -> Option<Decimal> {
        self.market_min_qty.or(self.min_qty)
    }

    pub fn effective_market_max_qty(&self) -> Option<Decimal> {
        self.market_max_qty.or(self.max_qty)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PositionState {
    pub qty: Decimal,
    pub entry_price: Option<Decimal>,
    pub entry_time_ms: Option<TimestampMs>,
    pub entry_order_id: Option<i64>,
}

impl PositionState {
    pub fn flat() -> Self {
        Self {
            qty: Decimal::ZERO,
            entry_price: None,
            entry_time_ms: None,
            entry_order_id: None,
        }
    }

    pub fn is_open(&self) -> bool {
        self.qty > Decimal::ZERO
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BotRunState {
    pub run_id: String,
    pub market: MarketId,
    pub strategy_name: String,
    pub strategy_config: serde_json::Value,
    pub status: RunStatus,
    pub last_processed_open_time_ms: Option<TimestampMs>,
    pub started_at_ms: TimestampMs,
    pub updated_at_ms: TimestampMs,
    pub stopped_at_ms: Option<TimestampMs>,
    pub last_error: Option<String>,
    pub position: PositionState,
}

#[derive(Error, Debug)]
pub enum ModelError {
    #[error("invalid symbol: {0}")]
    InvalidSymbol(String),

    #[error("invalid interval: {0}")]
    InvalidInterval(String),

    #[error("invalid side: {0}")]
    InvalidSide(String),

    #[error("invalid rfc3339 timestamp: {0}")]
    InvalidTimestamp(String),

    #[error("time parse error: {0}")]
    TimeParse(#[from] time::error::Parse),
}

pub fn parse_rfc3339_to_ms(input: &str) -> Result<TimestampMs, ModelError> {
    let input = input.trim();
    if input.is_empty() {
        return Err(ModelError::InvalidTimestamp("empty".to_string()));
    }
    let dt = OffsetDateTime::parse(input, &time::format_description::well_known::Rfc3339)?;
    Ok(dt.unix_timestamp() * 1000 + i64::from(dt.millisecond()))
}

pub fn ms_to_rfc3339(ms: TimestampMs) -> String {
    let seconds = ms.div_euclid(1000);
    let millis = ms.rem_euclid(1000) as u16;

    let dt = match OffsetDateTime::from_unix_timestamp(seconds) {
        Ok(dt) => match dt.replace_millisecond(millis) {
            Ok(adjusted) => adjusted,
            Err(_) => OffsetDateTime::UNIX_EPOCH,
        },
        Err(_) => OffsetDateTime::UNIX_EPOCH,
    };

    match dt.format(&time::format_description::well_known::Rfc3339) {
        Ok(value) => value,
        Err(_) => "1970-01-01T00:00:00Z".to_string(),
    }
}

pub fn now_utc_ms() -> TimestampMs {
    let now = OffsetDateTime::now_utc();
    now.unix_timestamp() * 1000 + i64::from(now.millisecond())
}

pub fn round_down_to_step(value: Decimal, step: Decimal) -> Decimal {
    if step <= Decimal::ZERO {
        return value;
    }
    (value / step).trunc() * step
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValidationIssue {
    OutOfOrder {
        prev_open_time_ms: TimestampMs,
        open_time_ms: TimestampMs,
    },
    DuplicateOpenTime {
        open_time_ms: TimestampMs,
    },
    Gap {
        expected_open_time_ms: TimestampMs,
        open_time_ms: TimestampMs,
    },
    OhlcInvalid {
        open_time_ms: TimestampMs,
        reason: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidationReport {
    pub market: MarketId,
    pub candle_count: usize,
    pub issues: Vec<ValidationIssue>,
}

impl ValidationReport {
    pub fn is_ok(&self) -> bool {
        self.issues.is_empty()
    }
}

pub fn validate_candles(market: &MarketId, candles: &[Candle]) -> ValidationReport {
    let mut issues = Vec::new();
    let mut seen = HashSet::<TimestampMs>::new();
    let step = market.interval.step_ms();
    let mut prev_open: Option<TimestampMs> = None;

    for candle in candles {
        if !seen.insert(candle.open_time_ms) {
            issues.push(ValidationIssue::DuplicateOpenTime {
                open_time_ms: candle.open_time_ms,
            });
        }

        if let Some(prev) = prev_open {
            if candle.open_time_ms <= prev {
                issues.push(ValidationIssue::OutOfOrder {
                    prev_open_time_ms: prev,
                    open_time_ms: candle.open_time_ms,
                });
            } else {
                let expected = prev + step;
                if candle.open_time_ms != expected {
                    issues.push(ValidationIssue::Gap {
                        expected_open_time_ms: expected,
                        open_time_ms: candle.open_time_ms,
                    });
                }
            }
        }

        if candle.low > candle.high {
            issues.push(ValidationIssue::OhlcInvalid {
                open_time_ms: candle.open_time_ms,
                reason: "low > high".to_string(),
            });
        }
        if candle.open < candle.low || candle.open > candle.high {
            issues.push(ValidationIssue::OhlcInvalid {
                open_time_ms: candle.open_time_ms,
                reason: "open not within [low, high]".to_string(),
            });
        }
        if candle.close < candle.low || candle.close > candle.high {
            issues.push(ValidationIssue::OhlcInvalid {
                open_time_ms: candle.open_time_ms,
                reason: "close not within [low, high]".to_string(),
            });
        }
        if candle.close_time_ms < candle.open_time_ms {
            issues.push(ValidationIssue::OhlcInvalid {
                open_time_ms: candle.open_time_ms,
                reason: "close_time_ms < open_time_ms".to_string(),
            });
        }

        prev_open = Some(candle.open_time_ms);
    }

    ValidationReport {
        market: market.clone(),
        candle_count: candles.len(),
        issues,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn market() -> MarketId {
        MarketId::new(
            ExchangeId::BinanceSpot,
            Symbol::new("BTCUSDT").expect("symbol"),
            Interval::M1,
        )
    }

    #[test]
    fn round_down_to_step_behaves() {
        assert_eq!(
            round_down_to_step(
                Decimal::from_str("1.234").expect("decimal"),
                Decimal::from_str("0.01").expect("decimal")
            ),
            Decimal::from_str("1.23").expect("decimal")
        );
    }

    #[test]
    fn validation_detects_gap() {
        let candles = vec![
            Candle {
                open_time_ms: 0,
                close_time_ms: 59_999,
                open: Decimal::ONE,
                high: Decimal::ONE,
                low: Decimal::ONE,
                close: Decimal::ONE,
                volume: Decimal::ONE,
                trades: Some(1),
            },
            Candle {
                open_time_ms: 120_000,
                close_time_ms: 179_999,
                open: Decimal::ONE,
                high: Decimal::ONE,
                low: Decimal::ONE,
                close: Decimal::ONE,
                volume: Decimal::ONE,
                trades: Some(1),
            },
        ];

        let report = validate_candles(&market(), &candles);
        assert_eq!(report.issues.len(), 1);
        assert!(matches!(report.issues[0], ValidationIssue::Gap { .. }));
    }

    #[test]
    fn order_average_price_falls_back_to_ratio() {
        let order = ExchangeOrder {
            symbol: Symbol::new("BTCUSDT").expect("symbol"),
            side: Side::Buy,
            order_type: "MARKET".to_string(),
            status: OrderStatus::Filled,
            order_id: Some(1),
            client_order_id: Some("abc".to_string()),
            requested_qty: None,
            requested_quote_qty: Some(Decimal::from_str("100").expect("decimal")),
            executed_qty: Decimal::from_str("0.01").expect("decimal"),
            cumulative_quote_qty: Decimal::from_str("100").expect("decimal"),
            avg_price: None,
            transact_time_ms: Some(1),
            fills: Vec::new(),
            raw: serde_json::json!({}),
        };

        assert_eq!(
            order.average_price(),
            Some(Decimal::from_str("10000").expect("decimal"))
        );
    }
}
