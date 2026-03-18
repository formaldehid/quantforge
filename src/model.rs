use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, fmt, str::FromStr};
use thiserror::Error;

pub type TimestampMs = i64;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Symbol(String);

impl Symbol {
    pub fn new(s: impl Into<String>) -> Result<Self, ModelError> {
        let s = s.into().trim().to_string();
        if s.is_empty() {
            return Err(ModelError::InvalidSymbol("empty".to_string()));
        }
        Ok(Self(s.to_ascii_uppercase()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
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
    M1,
    M5,
    H1,
    D1,
}

impl Interval {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::M1 => "1m",
            Self::M5 => "5m",
            Self::H1 => "1h",
            Self::D1 => "1d",
        }
    }

    pub fn step_ms(self) -> i64 {
        match self {
            Self::M1 => 60_000,
            Self::M5 => 300_000,
            Self::H1 => 3_600_000,
            Self::D1 => 86_400_000,
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
            "1m" => Ok(Self::M1),
            "5m" => Ok(Self::M5),
            "1h" => Ok(Self::H1),
            "1d" => Ok(Self::D1),
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

#[derive(Error, Debug)]
pub enum ModelError {
    #[error("invalid symbol: {0}")]
    InvalidSymbol(String),

    #[error("invalid interval: {0}")]
    InvalidInterval(String),

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

    let dt = time::OffsetDateTime::parse(input, &time::format_description::well_known::Rfc3339)?;
    Ok(dt.unix_timestamp() * 1000 + i64::from(dt.millisecond()))
}

pub fn ms_to_rfc3339(ms: TimestampMs) -> String {
    let seconds = ms.div_euclid(1000);
    let millis = ms.rem_euclid(1000) as u16;

    let dt = match time::OffsetDateTime::from_unix_timestamp(seconds) {
        Ok(dt) => match dt.replace_millisecond(millis) {
            Ok(adjusted) => adjusted,
            Err(_) => time::OffsetDateTime::UNIX_EPOCH,
        },
        Err(_) => time::OffsetDateTime::UNIX_EPOCH,
    };

    match dt.format(&time::format_description::well_known::Rfc3339) {
        Ok(s) => s,
        Err(_) => "1970-01-01T00:00:00Z".to_string(),
    }
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

    #[test]
    fn interval_parse_and_step() {
        assert_eq!(Interval::from_str("1m").expect("interval"), Interval::M1);
        assert_eq!(Interval::M5.step_ms(), 300_000);
    }

    #[test]
    fn parse_timestamp() {
        let ms = parse_rfc3339_to_ms("2024-01-01T00:00:00Z").expect("timestamp");
        assert_eq!(ms, 1_704_067_200_000);
    }

    #[test]
    fn detect_gap_and_duplicate() {
        let market = MarketId::new(
            ExchangeId::BinanceSpot,
            Symbol::new("BTCUSDT").expect("symbol"),
            Interval::M1,
        );
        let c0 = Candle {
            open_time_ms: 0,
            close_time_ms: 59_999,
            open: Decimal::from(1),
            high: Decimal::from(2),
            low: Decimal::from(1),
            close: Decimal::from(2),
            volume: Decimal::from(10),
            trades: Some(1),
        };
        let c1 = Candle {
            open_time_ms: 120_000,
            ..c0.clone()
        };
        let c2 = Candle {
            open_time_ms: 120_000,
            ..c0.clone()
        };

        let report = validate_candles(&market, &[c0, c1, c2]);
        assert!(!report.is_ok());
        assert!(
            report
                .issues
                .iter()
                .any(|issue| matches!(issue, ValidationIssue::Gap { .. }))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| matches!(issue, ValidationIssue::DuplicateOpenTime { .. }))
        );
    }
}
