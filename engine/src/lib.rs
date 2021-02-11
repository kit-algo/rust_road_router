#![allow(clippy::redundant_closure_call)]
#![allow(clippy::debug_assert_with_mut_call)]
#[macro_use]
extern crate scoped_tls;

macro_rules! dbg_each {
    ($($val:expr),+) => {
        (|| {
            $(
                dbg!($val);
            )+
        }) ()
    };
}

#[macro_use]
pub mod report;
pub mod algo;
pub mod cli;
pub mod datastr;
pub mod experiments;
pub mod export;
pub mod import;
pub mod io;
pub mod link_speed_estimates;
pub mod util;

mod as_mut_slice;
mod as_slice;

/// Build time information for experiments.
#[allow(dead_code)]
mod built_info {
    // The file has been placed there by the build script.
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}
