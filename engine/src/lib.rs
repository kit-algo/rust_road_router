#![feature(array_windows)]
#![feature(slice_group_by)]
#![feature(type_alias_impl_trait)]
#![feature(impl_trait_in_assoc_type)]
#![allow(clippy::collapsible_if)]
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
pub mod io;
pub mod link_speed_estimates;
pub mod util;

/// Build time information for experiments.
#[allow(dead_code)]
mod built_info {
    // The file has been placed there by the build script.
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}
