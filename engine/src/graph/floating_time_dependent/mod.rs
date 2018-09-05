use super::*;

mod piecewise_linear_function;
use self::piecewise_linear_function::*;

mod geometry;
use self::geometry::*;

mod graph;
pub use self::graph::Graph as TDGraph;

mod shortcut;
pub use self::shortcut::*;

mod shortcut_source;
use self::shortcut_source::*;

pub mod shortcut_graph;
pub use self::shortcut_graph::ShortcutGraph;
pub use self::shortcut_graph::SingleDirShortcutGraph;

// TODO switch to something ULP based?
// implications for division with EPSILON like divisors?
// const EPSILON: f64 = 0.000_000_1;

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Weight(f64);

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Timestamp(f64);

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

#[cfg(test)] use std::panic;
#[cfg(test)]
fn run_test_with_periodicity<T>(period: Timestamp, test: T) -> ()
    where T: FnOnce() -> () + panic::UnwindSafe
{
    unsafe { set_period(period) };

    let result = panic::catch_unwind(|| {
        test()
    });

    unsafe { reset_period() };

    assert!(result.is_ok())
}

#[cfg(test)]
pub fn period() -> Timestamp {
    return TEST_PERIOD_MOCK.with(|period_cell| period_cell.get().expect("period() used but not set"));
}

#[cfg(not(test))]
#[inline]
pub const fn period() -> Timestamp {
    Timestamp(86_400.0)
}
