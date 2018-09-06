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

#[allow(clippy::float_cmp)]
mod time {
    use std::{
        f64::NAN,
        ops::{Add, Sub},
        cmp::Ordering,
        borrow::Borrow,
    };
    use super::*;

    // TODO switch to something ULP based?
    // implications for division with EPSILON like divisors?
    // const EPSILON: f64 = 0.000_000_1;

    #[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
    pub struct FlWeight(f64);

    impl FlWeight {
        pub fn new(t: f64) -> Self {
            debug_assert_ne!(t, NAN);
            FlWeight(t)
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

    #[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
    pub struct Timestamp(f64);

    impl Timestamp {
        pub fn new(t: f64) -> Self {
            debug_assert_ne!(t, NAN);
            Timestamp(t)
        }

        pub const fn zero() -> Self {
            Timestamp(0.0)
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
            debug_assert!(result >= 0.0);
            debug_assert!(result <= period().0);
            Timestamp::new(result)
        }
    }

    impl<W: Borrow<FlWeight>> Sub<W> for Timestamp {
        type Output = Timestamp;

        fn sub(self, other: W) -> Self::Output {
            let result = self.0 - other.borrow().0;
            debug_assert!(result >= 0.0);
            debug_assert!(result <= period().0);
            Timestamp::new(result)
        }
    }
}
pub use self::time::*;

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
pub fn period() -> FlWeight {
    FlWeight::new(86_400.0)
}
