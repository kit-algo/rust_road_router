use super::*;

mod piecewise_linear_function;
use self::piecewise_linear_function::*;

mod geometry;
use self::geometry::*;
pub use self::geometry::TTFPoint;

mod graph;
pub use self::graph::Graph as TDGraph;

mod shortcut;
pub use self::shortcut::*;

mod shortcut_source;
use self::shortcut_source::*;

pub mod shortcut_graph;
pub use self::shortcut_graph::ShortcutGraph;
pub use self::shortcut_graph::CustomizedGraph;
pub use self::shortcut_graph::SingleDirBoundsGraph;

#[allow(clippy::float_cmp)]
mod time {
    use std::{
        f64::NAN,
        ops::{Add, Sub, Mul, Div},
        cmp::Ordering,
        borrow::Borrow,
    };

    // TODO switch to something ULP based?
    // implications for division with EPSILON like divisors?
    pub const EPSILON: f64 = 0.000_001;

    pub fn fuzzy_eq(x: f64, y: f64) -> bool {
        (x - y).abs() <= EPSILON
    }
    pub fn fuzzy_neq(x: f64, y: f64) -> bool {
        !fuzzy_eq(x, y)
    }
    pub fn fuzzy_lt(x: f64, y: f64) -> bool {
        x < y - EPSILON
    }

    #[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
    pub struct FlWeight(f64);

    pub const APPROX: FlWeight = FlWeight(10.0);

    impl FlWeight {
        pub const INFINITY: Self = FlWeight(2_147_483_647.0);

        pub fn new(t: f64) -> Self {
            debug_assert_ne!(t, NAN);
            FlWeight(t)
        }

        pub const fn zero() -> Self {
            FlWeight(0.0)
        }

        pub fn fuzzy_eq(self, other: Self) -> bool {
            fuzzy_eq(self.0, other.0)
        }
        pub fn fuzzy_lt(self, other: Self) -> bool {
            fuzzy_lt(self.0, other.0)
        }

        pub fn abs(self) -> FlWeight {
            FlWeight::new(self.0.abs())
        }
    }

    impl Eq for FlWeight {} // TODO ensure that the val will never be NAN

    impl Ord for FlWeight {
        fn cmp(&self, other: &Self) -> Ordering {
            self.partial_cmp(other).unwrap()
        }
    }

    impl<W: Borrow<FlWeight>> Add<W> for FlWeight {
        type Output = FlWeight;

        fn add(self, other: W) -> Self::Output {
            FlWeight::new(self.0 + other.borrow().0)
        }
    }

    impl<'a, W: Borrow<FlWeight>> Add<W> for &'a FlWeight {
        type Output = FlWeight;

        fn add(self, other: W) -> Self::Output {
            *self + other
        }
    }

    impl Add<Timestamp> for FlWeight {
        type Output = Timestamp;

        fn add(self, other: Timestamp) -> Self::Output {
            Timestamp::new(self.0 + other.0)
        }
    }

    impl Sub<FlWeight> for FlWeight {
        type Output = FlWeight;

        fn sub(self, other: FlWeight) -> Self::Output {
            FlWeight::new(self.0 - other.0)
        }
    }

    impl Mul<FlWeight> for FlWeight {
        type Output = FlWeight;

        fn mul(self, other: FlWeight) -> Self::Output {
            FlWeight::new(self.0 * other.0)
        }
    }

    impl Mul<FlWeight> for f64 {
        type Output = FlWeight;

        fn mul(self, other: FlWeight) -> Self::Output {
            FlWeight::new(self * other.0)
        }
    }

    impl Div<FlWeight> for FlWeight {
        type Output = FlWeight;

        fn div(self, other: FlWeight) -> Self::Output {
            debug_assert!(fuzzy_neq(other.0, 0.0));
            FlWeight::new(self.0 / other.0)
        }
    }

    impl<W: Borrow<Timestamp>> From<W> for FlWeight {
        fn from(w: W) -> Self {
            FlWeight::new(w.borrow().0)
        }
    }

    impl From<FlWeight> for f64 {
        fn from(w: FlWeight) -> Self {
            w.0
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
    pub struct Timestamp(f64);

    impl Timestamp {
        pub const NEVER: Self = Timestamp(2_147_483_647.0);

        pub fn new(t: f64) -> Self {
            debug_assert_ne!(t, NAN);
            Timestamp(t)
        }

        pub const fn zero() -> Self {
            Timestamp(0.0)
        }

        pub fn fuzzy_eq(self, other: Self) -> bool {
            fuzzy_eq(self.0, other.0)
        }
        pub fn fuzzy_lt(self, other: Self) -> bool {
            fuzzy_lt(self.0, other.0)
        }

        pub fn split_of_period(self) -> (FlWeight, Timestamp) {
            (FlWeight::new(self.0.div_euc(super::period().0)), Timestamp::new(self.0.mod_euc(super::period().0)))
        }
    }

    impl Eq for Timestamp {} // TODO ensure that the val will never be NAN

    impl Ord for Timestamp {
        fn cmp(&self, other: &Self) -> Ordering {
            self.partial_cmp(other).unwrap()
        }
    }

    impl<W: Borrow<FlWeight>> From<W> for Timestamp {
        fn from(w: W) -> Self {
            // TODO modulo period?
            Timestamp::new(w.borrow().0)
        }
    }

    impl<W: Borrow<FlWeight>> Add<W> for Timestamp {
        type Output = Timestamp;

        fn add(self, other: W) -> Self::Output {
            let result = self.0 + other.borrow().0;
            Timestamp::new(result)
        }
    }

    impl<W: Borrow<FlWeight>> Sub<W> for Timestamp {
        type Output = Timestamp;

        fn sub(self, other: W) -> Self::Output {
            let result = self.0 - other.borrow().0;
            Timestamp::new(result)
        }
    }

    impl Sub<Timestamp> for Timestamp {
        type Output = FlWeight;

        fn sub(self, other: Timestamp) -> Self::Output {
            FlWeight::new(self.0 - other.0)
        }
    }

    impl From<Timestamp> for f64 {
        fn from(t: Timestamp) -> Self {
            t.0
        }
    }
}
pub use self::time::*;

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
pub fn period() -> Timestamp {
    Timestamp::new(86_400.0)
}

thread_local! { pub static IPP_COUNT: Cell<usize> = Cell::new(0); }
thread_local! { pub static PATH_SOURCES_COUNT: Cell<usize> = Cell::new(0); }
thread_local! { pub static CLOSE_IPPS_COUNT: Cell<usize> = Cell::new(0); }
thread_local! { pub static ACTUALLY_MERGED: Cell<usize> = Cell::new(0); }
thread_local! { pub static ACTUALLY_LINKED: Cell<usize> = Cell::new(0); }
thread_local! { pub static ACTIVE_SHORTCUTS: Cell<usize> = Cell::new(0); }
thread_local! { pub static UNNECESSARY_LINKED: Cell<usize> = Cell::new(0); }
thread_local! { pub static CONSIDERED_FOR_APPROX: Cell<usize> = Cell::new(0); }
thread_local! { pub static SAVED_BY_APPROX: Cell<usize> = Cell::new(0); }
