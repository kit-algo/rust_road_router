//! Data structures for time-dependent routing with integer weights.
//!
//! The stuff in this module is actually not that interesting since it's just the
//! leftovers from early attempts on time-dependend CCHs.
//! What remains are the few parts that TD-S and TD-Dijkstra need.

use super::*;

mod piecewise_linear_function;
pub use self::piecewise_linear_function::*;

mod wrapping_range;
pub use self::wrapping_range::*;

mod graph;
pub use self::graph::Graph as TDGraph;
pub use self::graph::LiveTDGraph;

mod geometry;
use self::geometry::*;

pub mod math;
mod sorted_search_slice_ext;

/// A point in time.
pub type Timestamp = Weight;

#[cfg(test)]
use std::cell::Cell;

#[cfg(test)]
thread_local! {
    static TEST_PERIOD_MOCK: Cell<Option<Timestamp>> = Cell::new(None);
}

#[cfg(test)]
unsafe fn set_period(period: Timestamp) {
    TEST_PERIOD_MOCK.with(|period_cell| period_cell.set(Some(period)))
}

#[cfg(test)]
unsafe fn reset_period() {
    TEST_PERIOD_MOCK.with(|period_cell| period_cell.set(None))
}

#[cfg(test)]
use std::panic;
#[cfg(test)]
pub fn run_test_with_periodicity<T>(period: Timestamp, test: T)
where
    T: FnOnce() + panic::UnwindSafe,
{
    unsafe { set_period(period) };
    let result = panic::catch_unwind(test);
    unsafe { reset_period() };
    assert!(result.is_ok())
}

#[cfg(test)]
pub fn period() -> Timestamp {
    TEST_PERIOD_MOCK.with(|period_cell| period_cell.get().expect("period() used but not set"))
}

/// Travel time functions are periodic.
/// This value is the wraparound value.
/// Hardcoded to `86400s`, that is 1 day.
#[cfg(not(test))]
#[inline]
pub const fn period() -> Timestamp {
    86_400_000
}
