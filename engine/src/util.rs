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

/// Util function to chain unchainable function calls.
///
/// ```
/// # use bmw_routing_engine::util::TapOps;
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

#[derive(Debug)]
pub struct Slcs<'a, I, T> {
    first_elem: &'a [I],
    elements: &'a [T],
}

impl<'a, I, T> Slcs<'a, I, T> {
    pub fn new(first_elem: &'a [I], elements: &'a [T]) -> Self {
        Slcs { first_elem, elements }
    }
}

use std::convert::TryFrom;

impl<'a, I, T> std::ops::Index<usize> for Slcs<'a, I, T>
where
    usize: TryFrom<I>,
    <usize as std::convert::TryFrom<I>>::Error: std::fmt::Debug,
    I: Copy,
{
    type Output = [T];
    fn index(&self, idx: usize) -> &<Self as std::ops::Index<usize>>::Output {
        &self.elements[usize::try_from(self.first_elem[idx]).unwrap()..usize::try_from(self.first_elem[idx + 1]).unwrap()]
    }
}

#[derive(Debug)]
pub struct SlcsMut<'a, I, T> {
    first_elem: &'a [I],
    elements: &'a mut [T],
}

impl<'a, I, T> SlcsMut<'a, I, T> {
    pub fn new(first_elem: &'a [I], elements: &'a mut [T]) -> Self {
        SlcsMut { first_elem, elements }
    }
}

impl<'a, I, T> std::ops::Index<usize> for SlcsMut<'a, I, T>
where
    usize: TryFrom<I>,
    <usize as std::convert::TryFrom<I>>::Error: std::fmt::Debug,
    I: Copy,
{
    type Output = [T];
    fn index(&self, idx: usize) -> &<Self as std::ops::Index<usize>>::Output {
        &self.elements[usize::try_from(self.first_elem[idx]).unwrap()..usize::try_from(self.first_elem[idx + 1]).unwrap()]
    }
}

impl<'a, I, T> std::ops::IndexMut<usize> for SlcsMut<'a, I, T>
where
    usize: TryFrom<I>,
    <usize as std::convert::TryFrom<I>>::Error: std::fmt::Debug,
    I: Copy,
{
    fn index_mut(&mut self, idx: usize) -> &mut <Self as std::ops::Index<usize>>::Output {
        &mut self.elements[usize::try_from(self.first_elem[idx]).unwrap()..usize::try_from(self.first_elem[idx + 1]).unwrap()]
    }
}
