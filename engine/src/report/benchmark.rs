//! This module contains a few utilities to measure how long executing algorithms takes.
//! It utilizes the `time` crate.

use super::*;
use std::sync::atomic::{compiler_fence, Ordering::SeqCst};

/// This function will measure how long it takes to execute the given lambda,
/// print the time and return the result of the lambda.
pub fn report_time<Out, F: FnOnce() -> Out>(name: &str, f: F) -> Out {
    compiler_fence(SeqCst);
    let start = time::now();
    eprintln!("starting {}", name);
    let res = f();
    let t_passed = (time::now() - start).num_milliseconds();
    compiler_fence(SeqCst);
    eprintln!("{} done - took: {}ms", name, t_passed);
    report!("running_time_ms", t_passed);
    res
}

/// This function will measure how long it takes to execute the given lambda,
/// print the time, report it under the given key and return the result of the lambda.
pub fn report_time_with_key<Out, F: FnOnce() -> Out>(name: &str, key: &str, f: F) -> Out {
    compiler_fence(SeqCst);
    let start = time::now();
    eprintln!("starting {}", name);
    let res = f();
    let t_passed = (time::now() - start).num_milliseconds();
    compiler_fence(SeqCst);
    eprintln!("{} done - took: {}ms", name, t_passed);
    report!(format!("{}_running_time_ms", key), t_passed);
    res
}

/// This function will measure how long it takes to execute the given lambda,
/// print the time and return the result of the lambda.
pub fn silent_report_time<Out, F: FnOnce() -> Out>(f: F) -> Out {
    compiler_fence(SeqCst);
    let start = time::now();
    let res = f();
    let t_passed = (time::now() - start).num_milliseconds();
    compiler_fence(SeqCst);
    report_silent!("running_time_ms", t_passed);
    res
}

/// This function will measure how long it takes to execute the given lambda,
/// print the time, report it under the given key and return the result of the lambda.
pub fn silent_report_time_with_key<Out, F: FnOnce() -> Out>(key: &str, f: F) -> Out {
    compiler_fence(SeqCst);
    let start = time::now();
    let res = f();
    let t_passed = (time::now() - start).num_milliseconds();
    compiler_fence(SeqCst);
    report_silent!(format!("{}_running_time_ms", key), t_passed);
    res
}

/// This function will measure how long it takes to execute the given lambda
/// and return a tuple of the result of the lambda and a duration object.
pub fn measure<Out, F: FnOnce() -> Out>(f: F) -> (Out, time::Duration) {
    let start = time::now();
    let res = f();
    (res, time::now() - start)
}

/// A struct to repeatedly measure the time passed since the timer was started
#[derive(Debug)]
pub struct Timer {
    start: time::Tm,
}

impl Default for Timer {
    fn default() -> Self {
        Self::new()
    }
}

impl Timer {
    /// Create and start a new `Timer`
    pub fn new() -> Timer {
        Timer { start: time::now() }
    }

    /// Reset the `Timer`
    pub fn restart(&mut self) {
        self.start = time::now();
    }

    /// Print the passed time in ms since the timer was started
    pub fn report_passed_ms(&self) {
        eprintln!("{}ms", (time::now() - self.start).num_milliseconds());
    }

    /// Return the number of ms passed since the timer was started as a `i64`
    pub fn get_passed_ms(&self) -> i64 {
        (time::now() - self.start).num_milliseconds()
    }

    /// Return the number of ms passed since the timer was started as a Duration
    pub fn get_passed(&self) -> time::Duration {
        time::now() - self.start
    }
}
