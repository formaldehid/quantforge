use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, fmt, str::FromStr};
use thiserror::Error;
use time::OffsetDateTime;

pub type TimestampMs = i64;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct Symbol(String);

impl TryFrom<String> for Symbol {
    type Error = ModelError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

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
            Self::H8 => "8h",
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

/// Exchange-reported order state.
///
/// `executed_qty`, `cumulative_quote_qty`, and `fills` are `None` when the
/// exchange response did not report them (e.g. ACK-style responses, or the
/// open-orders endpoint which never includes fills). `None` means
/// "not reported" and is deliberately distinct from a reported zero.
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
    pub executed_qty: Option<Decimal>,
    pub cumulative_quote_qty: Option<Decimal>,
    pub avg_price: Option<Decimal>,
    pub transact_time_ms: Option<TimestampMs>,
    pub fills: Option<Vec<Fill>>,
    pub raw: serde_json::Value,
}

impl ExchangeOrder {
    pub fn average_price(&self) -> Option<Decimal> {
        if let Some(price) = self.avg_price {
            return Some(price);
        }
        match (self.executed_qty, self.cumulative_quote_qty) {
            (Some(executed), Some(cumulative)) if executed > Decimal::ZERO => {
                Some(cumulative / executed)
            }
            _ => None,
        }
    }

    /// Executed quantity minus fees charged in the base asset.
    ///
    /// Returns `None` when the exchange did not report the executed quantity
    /// or the fills, so callers must decide explicitly instead of assuming a
    /// fee-free fill. Fills without a `commission_asset` are treated as not
    /// charged in the base asset (under-deduction is surfaced by the fill
    /// itself, never invented).
    pub fn net_base_qty_after_base_fees(&self, base_asset: &str) -> Option<Decimal> {
        let executed = self.executed_qty?;
        let fills = self.fills.as_ref()?;
        let mut qty = executed;
        for fill in fills {
            if fill
                .commission_asset
                .as_deref()
                .map(|asset| asset.eq_ignore_ascii_case(base_asset))
                .unwrap_or(false)
            {
                qty -= fill.commission;
            }
        }
        Some(qty.max(Decimal::ZERO))
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
    pub execution_mode: ExecutionMode,
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

/// Formats epoch milliseconds as an RFC 3339 timestamp for display.
///
/// Values outside the representable datetime range render as an explicit
/// `invalid-ms(<value>)` marker instead of silently falling back to a
/// plausible-looking epoch date.
pub fn ms_to_rfc3339(ms: TimestampMs) -> String {
    let seconds = ms.div_euclid(1000);
    let millis = ms.rem_euclid(1000) as u16;

    OffsetDateTime::from_unix_timestamp(seconds)
        .ok()
        .and_then(|dt| dt.replace_millisecond(millis).ok())
        .and_then(|dt| {
            dt.format(&time::format_description::well_known::Rfc3339)
                .ok()
        })
        .unwrap_or_else(|| format!("invalid-ms({ms})"))
}

pub fn now_utc_ms() -> TimestampMs {
    let now = OffsetDateTime::now_utc();
    now.unix_timestamp() * 1000 + i64::from(now.millisecond())
}

/// Rounds `value` down to the nearest multiple of `step`.
///
/// `step` must be positive; passing a non-positive step is a caller bug.
/// Debug builds assert. Release builds return `value` unchanged so a bad
/// step can never manufacture a different quantity.
pub fn round_down_to_step(value: Decimal, step: Decimal) -> Decimal {
    debug_assert!(
        step > Decimal::ZERO,
        "round_down_to_step requires a positive step, got {step}"
    );
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
                match prev.checked_add(step) {
                    Some(expected) if candle.open_time_ms == expected => {}
                    Some(expected) => issues.push(ValidationIssue::Gap {
                        expected_open_time_ms: expected,
                        open_time_ms: candle.open_time_ms,
                    }),
                    // Past i64::MAX no representable open time is on-grid.
                    None => issues.push(ValidationIssue::Gap {
                        expected_open_time_ms: i64::MAX,
                        open_time_ms: candle.open_time_ms,
                    }),
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
        if candle.open <= Decimal::ZERO {
            issues.push(ValidationIssue::OhlcInvalid {
                open_time_ms: candle.open_time_ms,
                reason: "open <= 0".to_string(),
            });
        }
        if candle.high <= Decimal::ZERO {
            issues.push(ValidationIssue::OhlcInvalid {
                open_time_ms: candle.open_time_ms,
                reason: "high <= 0".to_string(),
            });
        }
        if candle.low <= Decimal::ZERO {
            issues.push(ValidationIssue::OhlcInvalid {
                open_time_ms: candle.open_time_ms,
                reason: "low <= 0".to_string(),
            });
        }
        if candle.close <= Decimal::ZERO {
            issues.push(ValidationIssue::OhlcInvalid {
                open_time_ms: candle.open_time_ms,
                reason: "close <= 0".to_string(),
            });
        }
        if candle.volume < Decimal::ZERO {
            issues.push(ValidationIssue::OhlcInvalid {
                open_time_ms: candle.open_time_ms,
                reason: "volume < 0".to_string(),
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

    // Saturating close_time so near-i64::MAX fixtures stay constructible.
    fn candle(open_time_ms: i64) -> Candle {
        Candle {
            open_time_ms,
            close_time_ms: open_time_ms.saturating_add(59_999),
            open: Decimal::ONE,
            high: Decimal::ONE,
            low: Decimal::ONE,
            close: Decimal::ONE,
            volume: Decimal::ONE,
            trades: Some(1),
        }
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
    fn ms_to_rfc3339_formats_valid_timestamps() {
        assert_eq!(ms_to_rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(ms_to_rfc3339(1_500), "1970-01-01T00:00:01.5Z");
    }

    #[test]
    fn ms_to_rfc3339_marks_out_of_range_timestamps_instead_of_epoch() {
        let rendered = ms_to_rfc3339(i64::MAX);
        assert_eq!(rendered, format!("invalid-ms({})", i64::MAX));
        let rendered = ms_to_rfc3339(i64::MIN);
        assert_eq!(rendered, format!("invalid-ms({})", i64::MIN));
    }

    #[test]
    fn validation_detects_gap() {
        let candles = vec![candle(0), candle(120_000)];

        let report = validate_candles(&market(), &candles);
        assert_eq!(report.issues.len(), 1);
        assert!(matches!(report.issues[0], ValidationIssue::Gap { .. }));
    }

    #[test]
    fn validation_accepts_contiguous_candles() {
        let candles = [candle(0), candle(60_000), candle(120_000)];

        let report = validate_candles(&market(), &candles);
        assert!(report.is_ok(), "got {:?}", report.issues);
        assert_eq!(report.candle_count, 3);
    }

    #[test]
    fn validation_accepts_empty_slice() {
        let report = validate_candles(&market(), &[]);
        assert!(report.is_ok());
        assert_eq!(report.candle_count, 0);
    }

    #[test]
    fn validation_accepts_single_candle() {
        let report = validate_candles(&market(), &[candle(0)]);
        assert!(report.is_ok(), "got {:?}", report.issues);
    }

    #[test]
    fn validation_detects_adjacent_duplicate_as_duplicate_and_out_of_order() {
        let candles = [candle(0), candle(0)];

        let report = validate_candles(&market(), &candles);
        assert_eq!(
            report.issues,
            vec![
                ValidationIssue::DuplicateOpenTime { open_time_ms: 0 },
                ValidationIssue::OutOfOrder {
                    prev_open_time_ms: 0,
                    open_time_ms: 0,
                },
            ]
        );
    }

    #[test]
    fn validation_detects_out_of_order_and_skips_gap_check() {
        let candles = [candle(120_000), candle(60_000)];

        let report = validate_candles(&market(), &candles);
        assert_eq!(
            report.issues,
            vec![ValidationIssue::OutOfOrder {
                prev_open_time_ms: 120_000,
                open_time_ms: 60_000,
            }]
        );
    }

    #[test]
    fn validation_advances_prev_after_out_of_order() {
        // The third candle is on-grid relative to the out-of-order second one,
        // so it raises only the duplicate issue.
        let candles = [candle(60_000), candle(0), candle(60_000)];

        let report = validate_candles(&market(), &candles);
        assert_eq!(
            report.issues,
            vec![
                ValidationIssue::OutOfOrder {
                    prev_open_time_ms: 60_000,
                    open_time_ms: 0,
                },
                ValidationIssue::DuplicateOpenTime {
                    open_time_ms: 60_000,
                },
            ]
        );
    }

    #[test]
    fn validation_resyncs_after_gap() {
        let candles = [candle(0), candle(120_000), candle(180_000)];

        let report = validate_candles(&market(), &candles);
        assert_eq!(
            report.issues,
            vec![ValidationIssue::Gap {
                expected_open_time_ms: 60_000,
                open_time_ms: 120_000,
            }]
        );
    }

    #[test]
    fn validation_detects_inverted_low_high_with_cascading_range_issues() {
        // An empty [low, high] range necessarily puts open and close outside
        // it, so "low > high" can never be the only issue.
        let candles = [Candle {
            low: Decimal::from(2),
            high: Decimal::ONE,
            open: Decimal::from_str("1.5").expect("decimal"),
            close: Decimal::from_str("1.5").expect("decimal"),
            ..candle(0)
        }];

        let report = validate_candles(&market(), &candles);
        assert_eq!(
            report.issues,
            vec![
                ValidationIssue::OhlcInvalid {
                    open_time_ms: 0,
                    reason: "low > high".to_string(),
                },
                ValidationIssue::OhlcInvalid {
                    open_time_ms: 0,
                    reason: "open not within [low, high]".to_string(),
                },
                ValidationIssue::OhlcInvalid {
                    open_time_ms: 0,
                    reason: "close not within [low, high]".to_string(),
                },
            ]
        );
    }

    #[test]
    fn validation_detects_open_outside_range() {
        let candles = [Candle {
            open: Decimal::from(3),
            high: Decimal::from(2),
            low: Decimal::ONE,
            close: Decimal::from_str("1.5").expect("decimal"),
            ..candle(0)
        }];

        let report = validate_candles(&market(), &candles);
        assert_eq!(
            report.issues,
            vec![ValidationIssue::OhlcInvalid {
                open_time_ms: 0,
                reason: "open not within [low, high]".to_string(),
            }]
        );
    }

    #[test]
    fn validation_detects_close_outside_range() {
        let candles = [Candle {
            open: Decimal::from_str("1.5").expect("decimal"),
            high: Decimal::from(2),
            low: Decimal::ONE,
            close: Decimal::from_str("0.5").expect("decimal"),
            ..candle(0)
        }];

        let report = validate_candles(&market(), &candles);
        assert_eq!(
            report.issues,
            vec![ValidationIssue::OhlcInvalid {
                open_time_ms: 0,
                reason: "close not within [low, high]".to_string(),
            }]
        );
    }

    #[test]
    fn validation_detects_close_time_before_open_time() {
        let candles = [Candle {
            close_time_ms: 59_999,
            ..candle(60_000)
        }];

        let report = validate_candles(&market(), &candles);
        assert_eq!(
            report.issues,
            vec![ValidationIssue::OhlcInvalid {
                open_time_ms: 60_000,
                reason: "close_time_ms < open_time_ms".to_string(),
            }]
        );
    }

    #[test]
    fn validation_accepts_candle_touching_its_own_bounds() {
        // Boundary equalities are valid: open == low, close == high,
        // close_time == open_time, and a zero-volume (quiet) bar.
        let candles = [Candle {
            open: Decimal::ONE,
            low: Decimal::ONE,
            close: Decimal::from(2),
            high: Decimal::from(2),
            close_time_ms: 0,
            volume: Decimal::ZERO,
            ..candle(0)
        }];

        let report = validate_candles(&market(), &candles);
        assert!(report.is_ok(), "got {:?}", report.issues);
    }

    #[test]
    fn validation_detects_non_positive_prices() {
        for raw in ["-1", "0"] {
            let value = Decimal::from_str(raw).expect("decimal");
            let candles = [Candle {
                open: value,
                high: value,
                low: value,
                close: value,
                ..candle(0)
            }];

            let report = validate_candles(&market(), &candles);
            assert_eq!(
                report.issues,
                vec![
                    ValidationIssue::OhlcInvalid {
                        open_time_ms: 0,
                        reason: "open <= 0".to_string(),
                    },
                    ValidationIssue::OhlcInvalid {
                        open_time_ms: 0,
                        reason: "high <= 0".to_string(),
                    },
                    ValidationIssue::OhlcInvalid {
                        open_time_ms: 0,
                        reason: "low <= 0".to_string(),
                    },
                    ValidationIssue::OhlcInvalid {
                        open_time_ms: 0,
                        reason: "close <= 0".to_string(),
                    },
                ],
                "for input {raw:?}"
            );
        }
    }

    #[test]
    fn validation_detects_negative_volume() {
        let candles = [Candle {
            volume: Decimal::from(-1),
            ..candle(0)
        }];

        let report = validate_candles(&market(), &candles);
        assert_eq!(
            report.issues,
            vec![ValidationIssue::OhlcInvalid {
                open_time_ms: 0,
                reason: "volume < 0".to_string(),
            }]
        );
    }

    #[test]
    fn validation_accepts_negative_open_time() {
        let candles = [candle(-120_000), candle(-60_000)];

        let report = validate_candles(&market(), &candles);
        assert!(report.is_ok(), "got {:?}", report.issues);
    }

    #[test]
    fn validation_flags_gap_at_i64_max_instead_of_overflowing() {
        let candles = [candle(i64::MAX - 1), candle(i64::MAX)];

        let report = validate_candles(&market(), &candles);
        assert_eq!(
            report.issues,
            vec![ValidationIssue::Gap {
                expected_open_time_ms: i64::MAX,
                open_time_ms: i64::MAX,
            }]
        );
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
            executed_qty: Some(Decimal::from_str("0.01").expect("decimal")),
            cumulative_quote_qty: Some(Decimal::from_str("100").expect("decimal")),
            avg_price: None,
            transact_time_ms: Some(1),
            fills: Some(Vec::new()),
            raw: serde_json::json!({}),
        };

        assert_eq!(
            order.average_price(),
            Some(Decimal::from_str("10000").expect("decimal"))
        );
    }

    /// Every `Interval` variant with the exact wire string Binance expects,
    /// the candle step duration in milliseconds, and the serde token (the
    /// JSON form persisted in `state_json`, distinct from the SQLite
    /// `interval` column which stores `as_str()`). Rows are ordered by
    /// ascending duration, matching enum declaration order. Steps are spelled
    /// as literal milliseconds so the table stays an independent oracle
    /// instead of mirroring the arithmetic in `step_ms()`.
    const INTERVAL_CASES: [(Interval, &str, i64, &str); 15] = [
        (Interval::S1, "1s", 1_000, "S1"),
        (Interval::M1, "1m", 60_000, "M1"),
        (Interval::M3, "3m", 180_000, "M3"),
        (Interval::M5, "5m", 300_000, "M5"),
        (Interval::M15, "15m", 900_000, "M15"),
        (Interval::M30, "30m", 1_800_000, "M30"),
        (Interval::H1, "1h", 3_600_000, "H1"),
        (Interval::H2, "2h", 7_200_000, "H2"),
        (Interval::H4, "4h", 14_400_000, "H4"),
        (Interval::H6, "6h", 21_600_000, "H6"),
        (Interval::H8, "8h", 28_800_000, "H8"),
        (Interval::H12, "12h", 43_200_000, "H12"),
        (Interval::D1, "1d", 86_400_000, "D1"),
        (Interval::D3, "3d", 259_200_000, "D3"),
        (Interval::W1, "1w", 604_800_000, "W1"),
    ];

    #[test]
    fn interval_cases_cover_every_variant() {
        let variants: HashSet<Interval> = INTERVAL_CASES
            .into_iter()
            .map(|(interval, ..)| interval)
            .collect();
        assert_eq!(variants.len(), 15);

        for interval in variants {
            // Exhaustive on purpose: a new variant stops this module from
            // compiling until it is added to INTERVAL_CASES.
            match interval {
                Interval::S1
                | Interval::M1
                | Interval::M3
                | Interval::M5
                | Interval::M15
                | Interval::M30
                | Interval::H1
                | Interval::H2
                | Interval::H4
                | Interval::H6
                | Interval::H8
                | Interval::H12
                | Interval::D1
                | Interval::D3
                | Interval::W1 => {}
            }
        }
    }

    #[test]
    fn interval_as_str_matches_exchange_wire_format() {
        for (interval, expected, ..) in INTERVAL_CASES {
            assert_eq!(interval.as_str(), expected, "for variant {interval:?}");
        }
    }

    #[test]
    fn interval_display_matches_as_str() {
        for (interval, expected, ..) in INTERVAL_CASES {
            assert_eq!(interval.to_string(), expected, "for variant {interval:?}");
        }
    }

    #[test]
    fn interval_wire_format_round_trips_through_parsing() {
        for (interval, expected, ..) in INTERVAL_CASES {
            assert_eq!(
                expected.parse::<Interval>().expect("interval"),
                interval,
                "for variant {interval:?}"
            );
            assert_eq!(
                interval.as_str().parse::<Interval>().expect("interval"),
                interval,
                "for variant {interval:?}"
            );
        }
    }

    #[test]
    fn interval_wire_formats_are_unique_per_variant() {
        let formatted: HashSet<&'static str> = INTERVAL_CASES
            .into_iter()
            .map(|(interval, ..)| interval.as_str())
            .collect();
        assert_eq!(formatted.len(), 15);
    }

    #[test]
    fn interval_step_ms_matches_expected_duration() {
        for (interval, _, expected_step_ms, _) in INTERVAL_CASES {
            assert_eq!(
                interval.step_ms(),
                expected_step_ms,
                "for variant {interval:?}"
            );
        }
    }

    #[test]
    fn interval_step_ms_strictly_increases_in_declaration_order() {
        for pair in INTERVAL_CASES.windows(2) {
            let (shorter, ..) = pair[0];
            let (longer, ..) = pair[1];
            assert!(
                shorter.step_ms() < longer.step_ms(),
                "expected {shorter:?} ({} ms) < {longer:?} ({} ms)",
                shorter.step_ms(),
                longer.step_ms()
            );
        }
    }

    #[test]
    fn interval_serde_json_round_trips_per_variant() {
        for (interval, _, _, serde_token) in INTERVAL_CASES {
            let json = serde_json::to_string(&interval).expect("serialize interval");
            assert_eq!(
                json,
                format!("\"{serde_token}\""),
                "for variant {interval:?}"
            );
            let parsed: Interval = serde_json::from_str(&json).expect("deserialize interval");
            assert_eq!(parsed, interval, "for variant {interval:?}");
        }
    }

    // The SQLite `interval` column stores `as_str()` ("8h") while `state_json`
    // stores the serde token ("H8"). QF-001 corrupted the wire channel (H8
    // formatted as "1m"); keep the two channels pinned as deliberately
    // distinct and mutually non-parseable so drift in either is caught.
    #[test]
    fn interval_serde_token_is_variant_name_not_wire_format() {
        for (interval, wire, ..) in INTERVAL_CASES {
            let json = serde_json::to_string(&interval).expect("serialize interval");
            assert_ne!(json, format!("\"{wire}\""), "for variant {interval:?}");
            assert!(
                serde_json::from_str::<Interval>(&format!("\"{wire}\"")).is_err(),
                "wire format {wire:?} must not deserialize as an Interval"
            );
        }
    }

    /// Rejected inputs with the exact error message each must produce.
    const INVALID_INTERVAL_CASES: [(&str, &str); 17] = [
        ("", "invalid interval: "),
        ("   ", "invalid interval: "),
        ("7m", "invalid interval: 7m"),
        (" 7m ", "invalid interval: 7m"),
        ("\t9x\n", "invalid interval: 9x"),
        ("8H", "invalid interval: 8H"),
        ("1S", "invalid interval: 1S"),
        ("1D", "invalid interval: 1D"),
        ("1W", "invalid interval: 1W"),
        ("H8", "invalid interval: H8"),
        ("60m", "invalid interval: 60m"),
        ("1min", "invalid interval: 1min"),
        ("2d", "invalid interval: 2d"),
        ("2w", "invalid interval: 2w"),
        ("1y", "invalid interval: 1y"),
        ("1M", "invalid interval: 1M"),
        ("8 h", "invalid interval: 8 h"),
    ];

    #[test]
    fn invalid_interval_strings_are_rejected_with_exact_messages() {
        for (input, expected_message) in INVALID_INTERVAL_CASES {
            let error = match input.parse::<Interval>() {
                Ok(parsed) => panic!("input {input:?} unexpectedly parsed as {parsed:?}"),
                Err(error) => error,
            };
            assert!(
                matches!(error, ModelError::InvalidInterval(_)),
                "for input {input:?}"
            );
            assert_eq!(error.to_string(), expected_message, "for input {input:?}");
        }
    }

    // Regression: QF-001, Interval::H8 formatted as "1m".
    #[test]
    fn h8_formats_as_eight_hours_and_not_one_minute() {
        assert_eq!(Interval::H8.as_str(), "8h");
        assert_eq!(Interval::H8.to_string(), "8h");
        assert_eq!("8h".parse::<Interval>().expect("interval"), Interval::H8);
    }

    #[test]
    fn surrounding_whitespace_is_trimmed_when_parsing() {
        assert_eq!(" 8h ".parse::<Interval>().expect("interval"), Interval::H8);
        assert_eq!(
            "\t1d\n".parse::<Interval>().expect("interval"),
            Interval::D1
        );
    }

    /// Inputs `Symbol::new` must accept, paired with the normalized form.
    const VALID_SYMBOL_CASES: [(&str, &str); 3] = [
        ("BTCUSDT", "BTCUSDT"),
        ("btcusdt", "BTCUSDT"),
        ("  ethusdt  ", "ETHUSDT"),
    ];

    #[test]
    fn symbol_new_trims_uppercases_and_preserves_valid_input() {
        for (input, expected) in VALID_SYMBOL_CASES {
            let symbol = Symbol::new(input).expect("symbol");
            assert_eq!(symbol.as_str(), expected, "for input {input:?}");
        }
    }

    #[test]
    fn empty_symbols_are_rejected_with_exact_message() {
        for input in ["", "   ", "\t\n"] {
            let error = Symbol::new(input).expect_err("symbol error");
            assert!(
                matches!(error, ModelError::InvalidSymbol(_)),
                "for input {input:?}"
            );
            assert_eq!(
                error.to_string(),
                "invalid symbol: empty",
                "for input {input:?}"
            );
        }
    }

    #[test]
    fn symbol_serializes_as_plain_string() {
        let symbol = Symbol::new("BTCUSDT").expect("symbol");
        let json = serde_json::to_string(&symbol).expect("serialize symbol");
        assert_eq!(json, "\"BTCUSDT\"");
    }

    // Deserialization must apply the same normalization as `Symbol::new`;
    // the derived impl used to bypass it entirely on `state_json`/`raw_json`
    // reloads from storage.
    #[test]
    fn symbol_deserialization_normalizes_like_new() {
        for (input, expected) in VALID_SYMBOL_CASES {
            let json = serde_json::to_string(input).expect("encode input");
            let symbol: Symbol = serde_json::from_str(&json).expect("deserialize symbol");
            assert_eq!(symbol.as_str(), expected, "for input {input:?}");
        }
    }

    #[test]
    fn symbol_deserialization_rejects_empty_with_model_error_message() {
        for raw in ["\"\"", "\"  \""] {
            let error = serde_json::from_str::<Symbol>(raw).expect_err("deserialize error");
            assert!(
                error.to_string().contains("invalid symbol: empty"),
                "for input {raw:?}, got {error}"
            );
        }
    }

    #[test]
    fn symbol_round_trips_through_serde_json() {
        let symbol = Symbol::new("BTCUSDT").expect("symbol");
        let json = serde_json::to_string(&symbol).expect("serialize symbol");
        let parsed: Symbol = serde_json::from_str(&json).expect("deserialize symbol");
        assert_eq!(parsed, symbol);
    }

    // `execution_mode` is part of a run's identity: state_json without it is
    // from an unsupported schema generation and must fail to load, never
    // default to a mode.
    #[test]
    fn bot_run_state_without_execution_mode_field_is_rejected() {
        let json = serde_json::json!({
            "run_id": "run-legacy",
            "market": {"exchange": "BinanceSpot", "symbol": "BTCUSDT", "interval": "M1"},
            "strategy_name": "sma_cross",
            "strategy_config": {"kind": "sma_cross", "fast": 20, "slow": 50},
            "status": "Running",
            "last_processed_open_time_ms": null,
            "started_at_ms": 0,
            "updated_at_ms": 0,
            "stopped_at_ms": null,
            "last_error": null,
            "position": {"qty": "0", "entry_price": null, "entry_time_ms": null, "entry_order_id": null}
        });
        let error = serde_json::from_value::<BotRunState>(json)
            .expect_err("state without execution_mode must not load");
        assert!(error.to_string().contains("execution_mode"), "got {error}");
    }

    #[test]
    fn market_id_json_with_lowercase_symbol_normalizes_on_deserialize() {
        let json = r#"{"exchange":"BinanceSpot","symbol":"btcusdt","interval":"M1"}"#;
        let market: MarketId = serde_json::from_str(json).expect("deserialize market");
        assert_eq!(market.symbol.as_str(), "BTCUSDT");
        assert_eq!(market.interval, Interval::M1);
    }
}
