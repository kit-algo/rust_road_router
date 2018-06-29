#![feature(compiler_fences)]
#![feature(const_fn)]
#![feature(allocator_api)]
#![feature(const_size_of)]
#![feature(mpsc_select)]
#![feature(euclidean_division)]
#![feature(range_contains)]

extern crate nav_types;
extern crate postgres;
extern crate csv;
extern crate glob;
extern crate time;
extern crate num_traits;

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
