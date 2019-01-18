#![feature(const_fn)]
#![feature(allocator_api)]
#![feature(euclidean_division)]
#![feature(range_contains)]
#![feature(nll)]

#[macro_use] pub mod report;
pub mod shortest_path;
pub mod graph;
pub mod io;
pub mod benchmark;
pub mod rank_select_map;
pub mod import;
pub mod link_speed_estimates;
pub mod export;

mod index_heap;
mod in_range_option;
mod as_slice;
mod as_mut_slice;
mod math;
mod sorted_search_slice_ext;

// Use of a mod or pub mod is not actually necessary.
pub mod built_info {
   // The file has been placed there by the build script.
   include!(concat!(env!("OUT_DIR"), "/built.rs"));
}
