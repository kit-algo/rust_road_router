This crate is a library of routing algorithms and related utilities.
For documentation on the API run `cargo doc --open`.
The debug directory contains a skeleton html app to visualize algorithms on a map for debugging.
`src` and `tests` contain source code and integration tests, respectively.

# Source code organization

The two main modules of this crate are the `algo` and the `datastr` modules.
Roughly speaking, `datastr` contains all data structures, `algo` all the algorithms.
However, the boundaries are not always perfectly clear.
For example, a good deal of the important algorithmic parts of CATCHUp are tied very closely to the data structures used and can be found in the `datastr::graph::floating_time_dependent` module.

The `algo` module is further divided into submodules for different speed-up techniques.
In `datastr`, the structs are build to be used across many different algorithms.

The `src/bin` directory contains a collection of binaries with utilities, experiments and tests for different algorithms.
These (specifically the `cch.rs` file) are good examples of how this library can be used.

# Implemented Algorithms

- **Dijkstra**: Basically all routing algorithms for road networks build on top of Dijkstra's algorithm. Thus, this crates contains many variants of this algorithm including a time-dependent version and a multicriteria version.
- **Contraction Hierarchies (CH)**: Graph contraction and fast query algorithms are implemented in `algo::contraction_hierarchy`. Node ordering is not implemented.
- **Customizable Contraction Hierarchies (CCH)**: A thoroughly engineered version of CCHs is provided in `algo::customizable_contraction_hierarchy`. Node orderings can be obtained with `IntertialFlowCutter`.
- **Time-dependent Sampling (TD-S)**: A lightweight heuristic for time-dependent routing, implemented in `algo::time_dependent_sampling`.
- **Customizable Approximated Time-dependent Contraction Hierarchies through Unpacking (CATCHUp)**: Code for the paper "Fast, exact and space-efficient routing in time-dependent road networks". `algo::catchup` contains only the query parts. Static preprocessing is the same as for CCHs. Customization parts are tied closely to the CCH customization and are implemented in `algo::customizable_contraction_hierarchy::customization::ftd`. Furthermore, many important parts are tied closely to the data structures and can be found in `datastr::graph::floating_time_dependent`.
- **CH Potentials**: Work In Progress, active research on perfect A* potentials for complicated problems.
