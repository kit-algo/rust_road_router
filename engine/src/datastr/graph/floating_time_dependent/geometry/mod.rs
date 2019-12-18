//! Algorithmic geometry related structs and methods

use super::*;

mod point;
pub use self::point::*;

#[cfg(feature = "tdcch-approx-imai-iri")]
mod imai_iri;
#[cfg(feature = "tdcch-approx-imai-iri")]
pub use self::imai_iri::*;
