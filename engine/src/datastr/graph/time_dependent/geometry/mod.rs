//! Too much type system around basic geoemtric operations.

use super::{Timestamp, Weight, INFINITY};

mod point;
pub use self::point::*;

mod line;
pub use self::line::*;

mod segment;
pub use self::segment::*;
