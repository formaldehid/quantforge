//! The strategy contract: the [`Strategy`] trait an engine drives bar by
//! bar, and the [`StrategyContext`] snapshot handed to every callback.
//! Everything crossing this boundary is owned plain data, so a foreign
//! implementation can sit behind an FFI boundary without borrowing into
//! engine internals.

use super::StrategyError;
use crate::model::{Candle, MarketId, TargetPosition, TimestampMs};
use rust_decimal::Decimal;

/// Plain-data snapshot of engine state passed to every [`Strategy`]
/// callback.
///
/// Every field is an owned value: nothing borrows into engine internals,
/// so a snapshot can be copied across an FFI boundary without lifetime
/// coupling to the engine that produced it.
///
/// Field provenance differs by engine and is part of the contract:
///
/// - `now_ms` is the current bar's open time in backtests and its close
///   time in live and dry runs (wall clock during `on_start` there)
/// - `cash` is the simulated quote balance in backtests and always zero in
///   live and dry runs, where order sizing comes from the configured
///   `quote_order_qty` instead
/// - `position_qty` is the engine's current base-asset position
#[derive(Clone, Debug, PartialEq)]
pub struct StrategyContext {
    pub market: MarketId,
    pub now_ms: TimestampMs,
    pub cash: Decimal,
    pub position_qty: Decimal,
}

/// A trading strategy driven bar by bar by an engine.
///
/// # Boundary contract
///
/// The trait is deliberately FFI-friendly: object-safe, no generics, no
/// lifetimes beyond transient borrows of owned data, input as a plain-data
/// snapshot ([`StrategyContext`]), and both decisions
/// (`Option<TargetPosition>`) and errors ([`StrategyError`], a message
/// string) as plain data. A foreign implementation only consumes values
/// and returns values; it never calls back into the engine.
///
/// ## Call order
///
/// 1. `on_start` — exactly once, before any bar
/// 2. `on_bar` — once per closed candle, in ascending open-time order;
///    engines never deliver a partial bar or the same bar twice
/// 3. `on_finish` — exactly once after the final bar, but not when an
///    earlier callback returned an error
///
/// ## Decision semantics
///
/// `on_bar` returns the desired position after this bar: `Some(target)`
/// requests it, `None` leaves the current position untouched. Requesting
/// the already-held target is a no-op. Backtests fill a request at the
/// next bar's open; live and dry runs execute against the signal bar's
/// close.
///
/// ## Error semantics
///
/// Returning `Err` from any callback aborts the run: engines stop
/// delivering bars, mark the run failed, and surface the message to the
/// operator.
///
/// ## Determinism
///
/// Implementations must be pure functions of the observed bar sequence and
/// their own accumulated state: no clocks, randomness, or I/O. The engines
/// rely on this to keep backtests reproducible and to warm strategies up
/// consistently when a live run restarts and replays recent bars.
pub trait Strategy: Send {
    /// Stable identifier recorded in run journals and operator output.
    ///
    /// Borrowed from `self` rather than `'static` so foreign strategies
    /// can report dynamically owned names.
    fn name(&self) -> &str;

    fn on_start(&mut self, _ctx: &StrategyContext) -> Result<(), StrategyError> {
        Ok(())
    }

    fn on_bar(
        &mut self,
        ctx: &StrategyContext,
        bar: &Candle,
    ) -> Result<Option<TargetPosition>, StrategyError>;

    fn on_finish(&mut self, _ctx: &StrategyContext) -> Result<(), StrategyError> {
        Ok(())
    }
}
