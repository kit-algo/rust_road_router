use super::*;

mod piecewise_linear_function;
use self::piecewise_linear_function::*;

mod wrapping_range;
pub use self::wrapping_range::*;

mod graph;
pub use self::graph::Graph as TDGraph;

mod geometry;
use self::geometry::*;

mod math;
mod sorted_search_slice_ext;

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
fn run_test_with_periodicity<T>(period: Timestamp, test: T)
where
    T: FnOnce() -> () + panic::UnwindSafe,
{
    unsafe { set_period(period) };

    let result = panic::catch_unwind(|| test());

    unsafe { reset_period() };

    assert!(result.is_ok())
}

#[cfg(test)]
pub fn period() -> Timestamp {
    TEST_PERIOD_MOCK.with(|period_cell| period_cell.get().expect("period() used but not set"))
}

#[cfg(not(test))]
#[inline]
pub const fn period() -> Timestamp {
    86_400_000
}

#[derive(Debug, Clone, Copy)]
pub enum ShortcutId {
    Outgoing(EdgeId),
    Incmoing(EdgeId),
}
