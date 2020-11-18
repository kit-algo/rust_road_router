//! Data structures for floating point based time dependent routing.
//!
//! Also contains most of the CATCHUp preprocessing, as its tied relatively closely to these data structures.
//! Customization logic lives in `algo::customizable_contraction_hierarchies::customization::ftd` but the interesting parts all happen here.
//!
//! We start with a `TDGraph` which contains the input.
//! After regular CCH static preprocessing we create a `Shortcut` for each edge in the CCH.
//! `Shortcut`s consist of several `ShortcutSource`s for different time ranges (but most of the time its just one source for the entire period).
//! `ShortcutSource`s can be empty, that is we currently have an infinity edge, or they can point to edges in the original graph,
//! or they can represent a shortcut, that is point to two other edges in the CCH which make up a lower triangle in the CCH with this shortcut.
//! Before customization all CCH edges will either be empty or point to an original edge.
//! During the customization, all lower triangles will be processed and the shortcuts will be modified such that in the end each shortcut for each point in time contains the shortest path.
//! That allows us, to later basically run a Contraction Hierarchy query.
//! For more details on the query see the [CATCHUP query module](crate::algo::catchup).

use super::*;

mod piecewise_linear_function;
pub use piecewise_linear_function::{PartialPiecewiseLinearFunction, PeriodicPiecewiseLinearFunction, UpdatedPiecewiseLinearFunction, PLF};

mod geometry;
pub use self::geometry::TTFPoint;
use self::geometry::*;

mod graph;
pub use self::graph::{Graph as TDGraph, LiveGraph, TDGraphTrait};

mod shortcut;
pub use self::shortcut::*;

mod live_shortcut;
pub use self::live_shortcut::*;

pub mod shortcut_source;
use self::shortcut_source::*;

pub mod shortcut_graph;
pub use self::shortcut_graph::*;

pub mod travel_time_function;
pub use travel_time_function::*;

#[allow(clippy::float_cmp)]
mod time {
    use std::{
        borrow::Borrow,
        cmp::Ordering,
        f64::NAN,
        ops::{Add, Div, Mul, Sub},
    };

    // TODO switch to something ULP based?
    // implications for division with EPSILON like divisors?
    /// Global epsilon for float comparisons
    pub const EPSILON: f64 = 0.000_001;

    fn fuzzy_eq(x: f64, y: f64) -> bool {
        (x - y).abs() <= EPSILON
    }
    fn fuzzy_neq(x: f64, y: f64) -> bool {
        !fuzzy_eq(x, y)
    }
    fn fuzzy_lt(x: f64, y: f64) -> bool {
        (x - y) < -EPSILON
    }
    fn fuzzy_leq(x: f64, y: f64) -> bool {
        !fuzzy_lt(y, x)
    }

    /// `f64` wrapper for time dependent edge weight values
    /// for some additional type safety.
    #[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
    pub struct FlWeight(f64);

    /// Absolute epsilon for CATCHUp approximation in seconds.
    /// Can be overriden through the TDCCH_APPROX env var
    #[cfg(not(override_tdcch_approx))]
    pub const APPROX: FlWeight = FlWeight(1.0);
    #[cfg(override_tdcch_approx)]
    pub const APPROX: FlWeight = FlWeight(include!(concat!(env!("OUT_DIR"), "/TDCCH_APPROX")));

    impl FlWeight {
        /// Sentinel value for infinity weights, chosen to match the regular `Weight`s `INFINITY`.
        pub const INFINITY: Self = FlWeight(2_147_483_647.0);

        /// New Weight from `f64`
        pub fn new(t: f64) -> Self {
            debug_assert_ne!(t, NAN);
            FlWeight(t)
        }

        /// Convenience function for zero weights
        pub const fn zero() -> Self {
            FlWeight(0.0)
        }

        /// Fuzzy comparison (based on `EPSILON`) of two weights
        pub fn fuzzy_eq(self, other: Self) -> bool {
            fuzzy_eq(self.0, other.0)
        }
        /// Fuzzy less than comparison (based on `EPSILON`) of two weights
        pub fn fuzzy_lt(self, other: Self) -> bool {
            fuzzy_lt(self.0, other.0)
        }
        pub fn fuzzy_leq(self, other: Self) -> bool {
            fuzzy_leq(self.0, other.0)
        }

        /// Take absolute value of this Weight
        pub fn abs(self) -> FlWeight {
            FlWeight::new(self.0.abs())
        }
    }

    impl Eq for FlWeight {} // TODO ensure that the val will never be NAN

    impl Ord for FlWeight {
        fn cmp(&self, other: &Self) -> Ordering {
            // Panic on NaN
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

    impl Default for FlWeight {
        fn default() -> Self {
            Self::zero()
        }
    }

    /// `f64` wrapper for points in time for additional type safety
    #[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
    pub struct Timestamp(f64);

    impl Timestamp {
        /// Sentinel value for infinity/never timestamps, chosen to match the regular `Weight`s `INFINITY`.
        pub const NEVER: Self = Timestamp(2_147_483_647.0);

        /// New `Timestamp` from `f64`.
        pub fn new(t: f64) -> Self {
            debug_assert_ne!(t, NAN);
            Timestamp(t)
        }

        /// Convenience function for zero `Timestamp`s
        pub const fn zero() -> Self {
            Timestamp(0.0)
        }

        /// Fuzzy equality comparison (based on `EPSILON`) of two timestamps
        pub fn fuzzy_eq(self, other: Self) -> bool {
            fuzzy_eq(self.0, other.0)
        }
        /// Fuzzy less than comparison (based on `EPSILON`) of two timestamps
        pub fn fuzzy_lt(self, other: Self) -> bool {
            fuzzy_lt(self.0, other.0)
        }
        pub fn fuzzy_leq(self, other: Self) -> bool {
            fuzzy_leq(self.0, other.0)
        }

        /// Split this value into sum of multiple of `period` (first value) and rest (second value).
        /// Negative values will be handled fine by using euclidian modulo and division.
        pub fn split_of_period(self) -> (FlWeight, Timestamp) {
            (
                FlWeight::new(self.0.div_euclid(super::period().0)),
                Timestamp::new(self.0.rem_euclid(super::period().0)),
            )
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

    impl Default for Timestamp {
        fn default() -> Self {
            Self::zero()
        }
    }
}
pub use self::time::{FlWeight, Timestamp, APPROX, EPSILON};

// Utils to allow tests to override `period` value.

#[cfg(test)]
thread_local! {
    static TEST_PERIOD_MOCK: std::cell::Cell<Option<Timestamp>> = std::cell::Cell::new(None);
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

/// Travel time functions are periodic. This value is the wraparound value.
/// Hardcoded to `86400s`, that is 1 day.
#[cfg(not(test))]
#[inline]
pub fn period() -> Timestamp {
    Timestamp::new(86_400.0)
}

use std::sync::atomic::{AtomicIsize, AtomicUsize};

// Stat counters for customization
pub static NODES_CUSTOMIZED: AtomicUsize = AtomicUsize::new(0);
pub static IPP_COUNT: AtomicUsize = AtomicUsize::new(0);
pub static PATH_SOURCES_COUNT: AtomicUsize = AtomicUsize::new(0);
pub static ACTUALLY_MERGED: AtomicUsize = AtomicUsize::new(0);
pub static ACTUALLY_LINKED: AtomicUsize = AtomicUsize::new(0);
pub static ACTIVE_SHORTCUTS: AtomicUsize = AtomicUsize::new(0);
pub static UNNECESSARY_LINKED: AtomicUsize = AtomicUsize::new(0);
pub static CONSIDERED_FOR_APPROX: AtomicUsize = AtomicUsize::new(0);
pub static SAVED_BY_APPROX: AtomicIsize = AtomicIsize::new(0);

/// Data structure to reduce allocations during customization.
/// Stores multiple PLFs consecutively in one `Vec`
/// The vector (capacity) grows on demand but should never shrink
#[derive(Debug)]
pub struct ReusablePLFStorage {
    data: Vec<TTFPoint>,
    first_points: Vec<u32>,
}

impl ReusablePLFStorage {
    fn new() -> Self {
        ReusablePLFStorage {
            data: Vec::new(),
            first_points: Vec::new(),
        }
    }

    /// Create a new PLF and push it on top of the existing ones in this object
    pub fn push_plf(&mut self) -> MutTopPLF {
        self.first_points.push(self.data.len() as u32);
        MutTopPLF { storage: self }
    }

    /// Get slices of the two topmost PLFs
    fn top_plfs(&self) -> (&[TTFPoint], &[TTFPoint]) {
        let num_plfs = self.first_points.len();
        (
            &self.data[self.first_points[num_plfs - 2] as usize..self.first_points[num_plfs - 1] as usize],
            &self.data[self.first_points[num_plfs - 1] as usize..],
        )
    }

    /// Get slice of the topmost PLF
    fn top_plf(&self) -> &[TTFPoint] {
        &self.data[self.first_points[self.first_points.len() - 1] as usize..]
    }
}

/// A wrapper with mutable access to a PLF in a `ReusablePLFStorage`.
/// Borrows the `ReusablePLFStorage` and allows other to reborrow it.
#[derive(Debug)]
pub struct MutTopPLF<'a> {
    storage: &'a mut ReusablePLFStorage,
}

impl<'a> MutTopPLF<'a> {
    fn storage(&self) -> &ReusablePLFStorage {
        &self.storage
    }

    fn storage_mut(&mut self) -> &mut ReusablePLFStorage {
        &mut self.storage
    }
}

impl<'a> PLFTarget for MutTopPLF<'a> {
    fn push(&mut self, p: TTFPoint) {
        if let Some(last) = self.storage.top_plf().last() {
            if last.at.fuzzy_lt(p.at) {
                self.storage.data.push(p);
            } else if last.at.fuzzy_eq(p.at) && last.val.fuzzy_eq(p.val) {
                return;
            } else {
                panic!("Trying to create non (time) monotone function");
            }
        } else {
            self.storage.data.push(p);
        }
    }

    fn pop(&mut self) -> Option<TTFPoint> {
        if self.is_empty() {
            None
        } else {
            self.storage.data.pop()
        }
    }
}

impl<'a> Extend<TTFPoint> for MutTopPLF<'a> {
    fn extend<T: IntoIterator<Item = TTFPoint>>(&mut self, iter: T) {
        self.storage.data.extend(iter);
    }
}

// Derefs to PLF slice
impl<'a> std::ops::Deref for MutTopPLF<'a> {
    type Target = [TTFPoint];

    fn deref(&self) -> &Self::Target {
        self.storage.top_plf()
    }
}

// When dropped, pops the PLF out of the storage
impl<'a> Drop for MutTopPLF<'a> {
    fn drop(&mut self) {
        self.storage.data.truncate(*self.storage.first_points.last().unwrap() as usize);
        self.storage.first_points.pop();
    }
}

/// Trait to abstract over different kinds of PLF storage
pub trait PLFTarget: Extend<TTFPoint> + std::ops::Deref<Target = [TTFPoint]> {
    /// Push single interpolation point to the PLF
    fn push(&mut self, val: TTFPoint);
    /// Pop single interpolation point from the PLF
    fn pop(&mut self) -> Option<TTFPoint>;
}

impl PLFTarget for Vec<TTFPoint> {
    fn push(&mut self, val: TTFPoint) {
        self.push(val);
    }

    fn pop(&mut self) -> Option<TTFPoint> {
        self.pop()
    }
}

/// Container struct which bundles all the reusable buffers we need during the customization for merging.
pub struct MergeBuffers {
    pub unpacking_target: ReusablePLFStorage,
    pub unpacking_tmp: ReusablePLFStorage,
    buffer: Vec<TTFPoint>,
    exact_result_lower: Vec<TTFPoint>,
    exact_result_upper: Vec<TTFPoint>,
}

impl Default for MergeBuffers {
    fn default() -> Self {
        Self::new()
    }
}

impl MergeBuffers {
    pub fn new() -> Self {
        MergeBuffers {
            unpacking_target: ReusablePLFStorage::new(),
            unpacking_tmp: ReusablePLFStorage::new(),
            buffer: Vec::new(),
            exact_result_lower: Vec::new(),
            exact_result_upper: Vec::new(),
        }
    }
}
