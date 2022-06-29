//! This module contains a few utilities to measure how long executing algorithms takes.
//! It utilizes the `time` crate.

use super::*;
use std::sync::atomic::{compiler_fence, Ordering::SeqCst};
use std::time::*;

/// This function will measure how long it takes to execute the given lambda,
/// print the time and return the result of the lambda.
pub fn report_time<Out, F: FnOnce() -> Out>(name: &str, f: F) -> Out {
    compiler_fence(SeqCst);
    let start = Instant::now();
    eprintln!("starting {}", name);
    let res = f();
    let t_passed = start.elapsed();
    compiler_fence(SeqCst);
    let t_passed = t_passed.as_secs_f64() * 1000.0;
    eprintln!("{} done - took: {}ms", name, t_passed);
    report!("running_time_ms", t_passed);
    res
}

/// This function will measure how long it takes to execute the given lambda,
/// print the time, report it under the given key and return the result of the lambda.
pub fn report_time_with_key<Out, F: FnOnce() -> Out>(name: &str, key: &'static str, f: F) -> Out {
    compiler_fence(SeqCst);
    let start = Instant::now();
    eprintln!("starting {}", name);
    let res = f();
    let t_passed = start.elapsed();
    compiler_fence(SeqCst);
    let t_passed = t_passed.as_secs_f64() * 1000.0;
    eprintln!("{} done - took: {}ms", name, t_passed);
    report!(key, t_passed);
    res
}

/// This function will measure how long it takes to execute the given lambda,
/// print the time and return the result of the lambda.
pub fn silent_report_time<Out, F: FnOnce() -> Out>(f: F) -> Out {
    compiler_fence(SeqCst);
    let start = Instant::now();
    let res = f();
    let t_passed = start.elapsed();
    compiler_fence(SeqCst);
    let t_passed = t_passed.as_secs_f64() * 1000.0;
    report_silent!("running_time_ms", t_passed);
    res
}

/// This function will measure how long it takes to execute the given lambda,
/// print the time, report it under the given key and return the result of the lambda.
pub fn silent_report_time_with_key<Out, F: FnOnce() -> Out>(key: &'static str, f: F) -> Out {
    compiler_fence(SeqCst);
    let start = Instant::now();
    let res = f();
    let t_passed = start.elapsed();
    compiler_fence(SeqCst);
    let t_passed = t_passed.as_secs_f64() * 1000.0;
    report_silent!(key, t_passed);
    res
}

/// This function will measure how long it takes to execute the given lambda
/// and return a tuple of the result of the lambda and a duration object.
pub fn measure<Out, F: FnOnce() -> Out>(f: F) -> (Out, Duration) {
    compiler_fence(SeqCst);
    let start = Instant::now();
    let res = f();
    let t_passed = start.elapsed();
    compiler_fence(SeqCst);
    (res, t_passed)
}

/// A struct to repeatedly measure the time passed since the timer was started
#[derive(Debug)]
pub struct Timer {
    start: Instant,
}

impl Default for Timer {
    fn default() -> Self {
        Self::new()
    }
}

impl Timer {
    /// Create and start a new `Timer`
    pub fn new() -> Timer {
        Timer { start: Instant::now() }
    }

    /// Reset the `Timer`
    pub fn restart(&mut self) {
        self.start = Instant::now();
    }

    /// Print the passed time in ms since the timer was started
    pub fn report_passed_ms(&self) {
        eprintln!("{}ms", self.start.elapsed().as_secs_f64() * 1000.0);
    }

    /// Return the number of ms passed since the timer was started as a `i64`
    pub fn get_passed_ms(&self) -> u128 {
        self.start.elapsed().as_millis()
    }

    /// Return the number of ms passed since the timer was started as a Duration
    pub fn get_passed(&self) -> Duration {
        self.start.elapsed()
    }
}
