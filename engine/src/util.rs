//! A random collection of small utilities.

use std::cmp::Ordering;

pub mod in_range_option;

/// Poor mans const generic bools, while waiting for actual support.
pub trait Bool {
    const VALUE: bool;
}

/// Poor mans const generic bool true.
pub struct True;

impl Bool for True {
    const VALUE: bool = true;
}

/// Poor mans const generic bool false.
pub struct False;

impl Bool for False {
    const VALUE: bool = false;
}

/// Non NaN float wrapper for sorting floats
/// (Yes rust, I know floats are dangerous but I have to sort them anyway.)
#[derive(PartialEq, PartialOrd)]
pub struct NonNan(f32);

impl NonNan {
    pub fn new(val: f32) -> Option<NonNan> {
        if val.is_nan() {
            None
        } else {
            Some(NonNan(val))
        }
    }
}

impl Eq for NonNan {}

impl Ord for NonNan {
    fn cmp(&self, other: &NonNan) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

pub fn fl_min(x: f64, y: f64) -> f64 {
    if x < y {
        x
    } else {
        y
    }
}

pub fn fl_max(x: f64, y: f64) -> f64 {
    if x > y {
        x
    } else {
        y
    }
}

/// Util function to chain unchainable function calls.
///
/// ```
/// # use rust_road_router::util::TapOps;
/// assert_eq!(vec![2,7,4].tap(|slf| slf.sort()).tap(|slf| slf.pop()), vec![2,4]);
/// ```
pub trait TapOps: Sized {
    fn tap<R, F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut Self) -> R,
    {
        let _ = f(&mut self);
        self
    }
}

impl<T> TapOps for T where T: Sized {}

use std::convert::TryFrom;

#[derive(Clone)]
pub struct Vecs<T> {
    first_idx: Vec<usize>,
    data: Vec<T>,
}

impl<T> Vecs<T> {
    pub fn empty() -> Self {
        Self {
            first_idx: vec![0],
            data: Vec::new(),
        }
    }

    pub fn from_iters<Inner: Iterator<Item = T>, Outer: Iterator<Item = Inner>>(iters: Outer) -> Self {
        let mut first_idx = Vec::with_capacity(iters.size_hint().0 + 1);
        first_idx.push(0);
        let mut data = Vec::new();

        for iter in iters {
            data.extend(iter);
            first_idx.push(data.len());
        }

        Self { first_idx, data }
    }

    pub fn iter(&self) -> impl Iterator<Item = &[T]> {
        self.first_idx.array_windows::<2>().map(|&[from, to]| &self.data[from..to])
    }
}

use rayon::prelude::*;

impl<T> Vecs<T>
where
    T: Sync,
{
    pub fn par_iter(&self) -> impl IndexedParallelIterator<Item = &[T]> {
        self.first_idx.par_windows(2).map(|idxs| &self.data[idxs[0]..idxs[1]])
    }
}

impl<T> std::ops::Index<usize> for Vecs<T> {
    type Output = [T];
    fn index(&self, idx: usize) -> &<Self as std::ops::Index<usize>>::Output {
        &self.data[SlcsIdx(&self.first_idx).range(idx)]
    }
}

pub struct SlcsIdx<'a, Idx>(pub &'a [Idx]);

impl<'a, Idx> SlcsIdx<'a, Idx>
where
    usize: TryFrom<Idx>,
    <usize as std::convert::TryFrom<Idx>>::Error: std::fmt::Debug,
    Idx: Copy,
{
    pub fn range(&self, idx: usize) -> std::ops::Range<usize> {
        usize::try_from(self.0[idx]).unwrap()..usize::try_from(self.0[idx + 1]).unwrap()
    }
}

pub struct Slcs<'a, I, T>(pub &'a [I], pub &'a [T]);

impl<'a, I, T> Slcs<'a, I, T>
where
    usize: TryFrom<I>,
    <usize as std::convert::TryFrom<I>>::Error: std::fmt::Debug,
    I: Copy,
{
    pub fn range(&self, idx: usize) -> std::ops::Range<usize> {
        SlcsIdx(self.0).range(idx)
    }
}

impl<'a, I, T> std::ops::Index<usize> for Slcs<'a, I, T>
where
    usize: TryFrom<I>,
    <usize as std::convert::TryFrom<I>>::Error: std::fmt::Debug,
    I: Copy,
{
    type Output = [T];
    fn index(&self, idx: usize) -> &<Self as std::ops::Index<usize>>::Output {
        &self.1[SlcsIdx(self.0).range(idx)]
    }
}

pub struct SlcsMut<'a, I, T>(pub &'a [I], pub &'a mut [T]);

impl<'a, I, T> std::ops::Index<usize> for SlcsMut<'a, I, T>
where
    usize: TryFrom<I>,
    <usize as std::convert::TryFrom<I>>::Error: std::fmt::Debug,
    I: Copy,
{
    type Output = [T];
    fn index(&self, idx: usize) -> &<Self as std::ops::Index<usize>>::Output {
        &self.1[SlcsIdx(self.0).range(idx)]
    }
}

impl<'a, I, T> std::ops::IndexMut<usize> for SlcsMut<'a, I, T>
where
    usize: TryFrom<I>,
    <usize as std::convert::TryFrom<I>>::Error: std::fmt::Debug,
    I: Copy,
{
    fn index_mut(&mut self, idx: usize) -> &mut <Self as std::ops::Index<usize>>::Output {
        &mut self.1[SlcsIdx(self.0).range(idx)]
    }
}

use std::iter::Fuse;

/// An iterator adaptor that alternates elements from two iterators until both
/// run out.
///
/// This iterator is *fused*.
///
/// See [`.interleave()`](../trait.Itertools.html#method.interleave) for more information.
#[derive(Clone, Debug)]
#[must_use = "iterator adaptors are lazy and do nothing unless consumed"]
pub struct Interleave<I, J> {
    a: Fuse<I>,
    b: Fuse<J>,
    flag: bool,
}

/// Create an iterator that interleaves elements in `i` and `j`.
///
/// `IntoIterator` enabled version of `i.interleave(j)`.
///
/// See [`.interleave()`](trait.Itertools.html#method.interleave) for more information.
pub fn interleave<I, J>(i: I, j: J) -> Interleave<<I as IntoIterator>::IntoIter, <J as IntoIterator>::IntoIter>
where
    I: IntoIterator,
    J: IntoIterator<Item = I::Item>,
{
    Interleave {
        a: i.into_iter().fuse(),
        b: j.into_iter().fuse(),
        flag: false,
    }
}

impl<I, J> Iterator for Interleave<I, J>
where
    I: Iterator,
    J: Iterator<Item = I::Item>,
{
    type Item = I::Item;
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.flag = !self.flag;
        if self.flag {
            match self.a.next() {
                None => self.b.next(),
                r => r,
            }
        } else {
            match self.b.next() {
                None => self.a.next(),
                r => r,
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        size_hint_add(self.a.size_hint(), self.b.size_hint())
    }
}

#[inline]
fn size_hint_add(a: (usize, Option<usize>), b: (usize, Option<usize>)) -> (usize, Option<usize>) {
    let min = a.0.saturating_add(b.0);
    let max = match (a.1, b.1) {
        (Some(x), Some(y)) => x.checked_add(y),
        _ => None,
    };

    (min, max)
}

pub fn coordinated_sweep_iter<T, I, J>(first: I, second: J) -> CoordinateSweepIter<I, J>
where
    T: Ord,
    I: Iterator<Item = T>,
    J: Iterator<Item = T>,
{
    CoordinateSweepIter {
        first: first.peekable(),
        second: second.peekable(),
    }
}

pub struct CoordinateSweepIter<I: Iterator, J: Iterator> {
    first: std::iter::Peekable<I>,
    second: std::iter::Peekable<J>,
}

impl<T, I, J> Iterator for CoordinateSweepIter<I, J>
where
    T: Ord,
    I: Iterator<Item = T>,
    J: Iterator<Item = T>,
{
    type Item = (Option<T>, Option<T>);

    fn next(&mut self) -> Option<Self::Item> {
        use std::cmp::Ordering::*;
        match (self.first.peek(), self.second.peek()) {
            (Some(first), Some(second)) => match first.cmp(second) {
                Less => Some((self.first.next(), None)),
                Greater => Some((None, self.second.next())),
                Equal => Some((self.first.next(), self.second.next())),
            },
            (Some(_), None) => Some((self.first.next(), None)),
            (None, Some(_)) => Some((None, self.second.next())),
            (None, None) => None,
        }
    }
}

pub trait MyFrom<T>: Sized {
    /// Performs the conversion.
    fn mfrom(_: T) -> Self;
}

pub trait MyInto<T>: Sized {
    /// Performs the conversion.
    fn minto(self) -> T;
}

impl<T, U> MyInto<U> for T
where
    U: MyFrom<T>,
{
    fn minto(self) -> U {
        U::mfrom(self)
    }
}

pub fn with_index<T, F>(mut f: F) -> impl FnMut(&T) -> bool
where
    F: FnMut(usize, &T) -> bool,
{
    let mut i = 0;
    move |item| (f(i, item), i += 1).0
}
