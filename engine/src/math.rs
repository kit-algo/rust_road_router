type Num = u32;

pub fn gcd(mut a: Num, mut b: Num) -> Num {
    while b != 0 {
        let tmp = a % b;
        a = b;
        b = tmp;
    }
    a
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LinearCongruenceSolution {
    pub solution: Num,
    pub modulus: Num
}

pub fn solve_linear_congruence(factor: Num, rest: Num, modulus: Num) -> Option<LinearCongruenceSolution> {
    let (gcd, s, _t) = extended_euklid(factor, modulus);
    if rest % gcd != 0 { return None };
    let mut solution = s * i64::from(rest) / i64::from(gcd);
    let modulus = modulus / gcd;

    if solution < 0 {
        solution += (1 - (solution / i64::from(modulus))) * i64::from(modulus);
    }

    let solution = solution as Num;
    Some(LinearCongruenceSolution { solution, modulus })
}

fn extended_euklid(a: Num, b: Num) -> (Num, i64, i64) {
    let mut s: i64 = 0;
    let mut old_s: i64 = 1;
    let mut t: i64 = 1;
    let mut old_t: i64 = 0;
    let mut remainder = i64::from(b);
    let mut old_remainder = i64::from(a);
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
    debug_assert_eq!(old_remainder, i64::from(a) * old_s + i64::from(b) * old_t);
    (old_remainder as Num, old_s, old_t)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gcd() {
        assert_eq!(gcd(6, 9), 3);
        assert_eq!(gcd(2, 3), 1);
        assert_eq!(gcd(4, 2), 2);
        assert_eq!(gcd(4, 8), 4);
        assert_eq!(gcd(20, 12), 4);
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
