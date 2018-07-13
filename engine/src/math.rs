#[cfg(test)]
use num_traits::Unsigned;

#[cfg(test)]
pub fn gcd<Num: Unsigned + Copy>(mut a: Num, mut b: Num) -> Num {
    while b != Num::zero() {
        let tmp = a % b;
        a = b;
        b = tmp;
    }
    a
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LinearCongruenceSolution {
    pub solution: i64,
    pub modulus: u64
}

pub fn solve_linear_congruence(factor: i64, rest: i64, modulus: i64) -> Option<LinearCongruenceSolution> {
    let (gcd, s, _t) = extended_euklid(factor, modulus);
    if rest % gcd != 0 { return None };
    let mut solution = s * rest / gcd;
    let modulus = modulus / gcd;

    debug_assert!(modulus > 0);
    solution = solution.mod_euc(modulus);

    debug_assert!(solution >= 0);
    Some(LinearCongruenceSolution { solution, modulus: modulus as u64 })
}

fn extended_euklid(a: i64, b: i64) -> (i64, i64, i64) {
    let mut s: i64 = 0;
    let mut old_s: i64 = 1;
    let mut t: i64 = 1;
    let mut old_t: i64 = 0;
    let mut remainder = b;
    let mut old_remainder = a;
    let mut tmp;
    while remainder != 0 {
        let quotient = old_remainder / remainder;

        tmp = old_remainder - quotient * remainder;
        old_remainder = remainder;
        remainder = tmp;

        tmp = old_s - quotient * s;
        old_s = s;
        s = tmp;

        tmp = old_t - quotient * t;
        old_t = t;
        t = tmp;
    }
    debug_assert_eq!(old_remainder, a * old_s + b * old_t);
    (old_remainder, old_s, old_t)
}

use std::ops::Range;
use std::cmp::{max, min};

pub trait RangeExtensions {
    fn intersection(&self, other: &Self) -> Self;
    fn is_intersection_empty(&self, other: &Self) -> bool;
}

impl<T: Ord + Copy> RangeExtensions for Range<T> {
    fn intersection(&self, other: &Self) -> Self {
        Range { start: max(self.start, other.start), end: min(self.end, other.end) }
    }

    fn is_intersection_empty(&self, other: &Self) -> bool {
        let intersection = self.intersection(other);
        intersection.start >= intersection.end
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gcd() {
        assert_eq!(gcd::<u32>(6, 9), 3);
        assert_eq!(gcd::<u32>(2, 3), 1);
        assert_eq!(gcd::<u32>(4, 2), 2);
        assert_eq!(gcd::<u32>(4, 8), 4);
        assert_eq!(gcd::<u32>(20, 12), 4);
    }

    #[test]
    fn test_solving_linear_congruence() {
        // assert_eq!(solve_linear_congruence(3, 1, 3), None);
        assert_eq!(solve_linear_congruence(2, 2, 3), Some(LinearCongruenceSolution { solution: 1, modulus: 3 }));
        assert_eq!(solve_linear_congruence(4, 10, 18), Some(LinearCongruenceSolution { solution: 7, modulus: 9 }));
    }

    #[test]
    fn test_extended_euklid() {
        assert_eq!(extended_euklid(6, 9).0, 3);
        assert_eq!(extended_euklid(2, 3).0, 1);
        assert_eq!(extended_euklid(4, 2).0, 2);
        assert_eq!(extended_euklid(4, 8).0, 4);
        assert_eq!(extended_euklid(20, 12).0, 4);
    }
}
