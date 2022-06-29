//! Utilities for structured reporting of experimental results.
//!
//! Experimental take on an API using RAII to report experimental results within context
//! somewhat isomorph to the callgraph and output everything as JSON.
//!
//! So far not really successful.
//! While it worked quite well for the CATCHUp experiments, the API is not really robust.
//! Keeping the ContextGuards around pollutes the algorithm code and is a bit error prone.
//! When used in a multithreaded environment, weird stuff will happen.
//! Not really ready for productive use.
//! JSON output is nice though.

use crate::built_info;
use serde::{Serialize, Serializer};
use serde_json::Value;
use std::collections::BTreeMap;
use std::{cell::RefCell, mem::swap};

pub use serde_json::json;

pub enum ReportingValue {
    Collection(Vec<ReportingValue>),
    Object(BTreeMap<&'static str, ReportingValue>),
    Value(Value),
}

impl Serialize for ReportingValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            ReportingValue::Collection(v) => v.serialize(serializer),
            ReportingValue::Object(v) => v.serialize(serializer),
            ReportingValue::Value(v) => v.serialize(serializer),
        }
    }
}

enum ContextStackItem {
    Key(&'static str),
    Collection(Vec<ReportingValue>),
    Object(BTreeMap<&'static str, ReportingValue>),
    Capture(BTreeMap<&'static str, ReportingValue>),
    Throwaway,
}

enum CurrentReportingContext {
    Collection(Vec<ReportingValue>),
    Object(BTreeMap<&'static str, ReportingValue>),
    Capture(BTreeMap<&'static str, ReportingValue>),
    Throwaway,
}

pub struct Reporter {
    current: CurrentReportingContext,
    context_stack: Vec<ContextStackItem>,
}

impl Default for Reporter {
    fn default() -> Self {
        Reporter {
            current: CurrentReportingContext::Object(BTreeMap::new()),
            context_stack: Vec::new(),
        }
    }
}

impl Reporter {
    fn create_object_under_key(&mut self, key: &'static str) {
        match &mut self.current {
            CurrentReportingContext::Object(object) => {
                let mut tmp = BTreeMap::new();
                swap(&mut tmp, object);
                self.context_stack.push(ContextStackItem::Object(tmp));
                self.context_stack.push(ContextStackItem::Key(key));
            }
            CurrentReportingContext::Capture(object) => {
                let mut tmp = BTreeMap::new();
                swap(&mut tmp, object);
                self.context_stack.push(ContextStackItem::Capture(tmp));
                self.context_stack.push(ContextStackItem::Key(key));
            }
            CurrentReportingContext::Collection(_) => {
                panic!("Cannot create object at key in collection");
            }
            CurrentReportingContext::Throwaway => (),
        }
    }

    fn create_collection_under_key(&mut self, key: &'static str) {
        match &mut self.current {
            CurrentReportingContext::Object(object) => {
                let mut tmp = BTreeMap::new();
                swap(&mut tmp, object);
                self.context_stack.push(ContextStackItem::Object(tmp));
                self.context_stack.push(ContextStackItem::Key(key));
                self.current = CurrentReportingContext::Collection(Vec::new());
            }
            CurrentReportingContext::Capture(object) => {
                let mut tmp = BTreeMap::new();
                swap(&mut tmp, object);
                self.context_stack.push(ContextStackItem::Capture(tmp));
                self.context_stack.push(ContextStackItem::Key(key));
                self.current = CurrentReportingContext::Collection(Vec::new());
            }
            CurrentReportingContext::Collection(_) => {
                panic!("Cannot create collection at key in collection");
            }
            CurrentReportingContext::Throwaway => (),
        }
    }

    fn create_collection_item(&mut self) {
        match &mut self.current {
            CurrentReportingContext::Object(_) | CurrentReportingContext::Capture(_) => {
                panic!("Cannot create collection item in object");
            }
            CurrentReportingContext::Collection(collection) => {
                let mut tmp = Vec::new();
                swap(&mut tmp, collection);
                self.context_stack.push(ContextStackItem::Collection(tmp));
                self.current = CurrentReportingContext::Object(BTreeMap::new());
            }
            CurrentReportingContext::Throwaway => (),
        }
    }

    fn block_reporting(&mut self) {
        match &mut self.current {
            CurrentReportingContext::Object(object) => {
                let mut tmp = BTreeMap::new();
                swap(&mut tmp, object);
                self.context_stack.push(ContextStackItem::Object(tmp));
                self.current = CurrentReportingContext::Throwaway;
            }
            CurrentReportingContext::Capture(object) => {
                let mut tmp = BTreeMap::new();
                swap(&mut tmp, object);
                self.context_stack.push(ContextStackItem::Capture(tmp));
                self.current = CurrentReportingContext::Throwaway;
            }
            CurrentReportingContext::Collection(collection) => {
                let mut tmp = Vec::new();
                swap(&mut tmp, collection);
                self.context_stack.push(ContextStackItem::Collection(tmp));
                self.current = CurrentReportingContext::Throwaway;
            }
            CurrentReportingContext::Throwaway => {
                self.context_stack.push(ContextStackItem::Throwaway);
            }
        }
    }

    fn capture_reporting(&mut self) {
        match &mut self.current {
            CurrentReportingContext::Object(object) => {
                let mut tmp = BTreeMap::new();
                swap(&mut tmp, object);
                self.context_stack.push(ContextStackItem::Object(tmp));
                self.current = CurrentReportingContext::Capture(BTreeMap::new());
            }
            CurrentReportingContext::Capture(object) => {
                let mut tmp = BTreeMap::new();
                swap(&mut tmp, object);
                self.context_stack.push(ContextStackItem::Capture(tmp));
                self.current = CurrentReportingContext::Capture(BTreeMap::new());
            }
            CurrentReportingContext::Collection(collection) => {
                let mut tmp = Vec::new();
                swap(&mut tmp, collection);
                self.context_stack.push(ContextStackItem::Collection(tmp));
                self.current = CurrentReportingContext::Capture(BTreeMap::new());
            }
            CurrentReportingContext::Throwaway => {
                self.context_stack.push(ContextStackItem::Throwaway);
                self.current = CurrentReportingContext::Capture(BTreeMap::new());
            }
        }
    }

    fn report(&mut self, key: &'static str, val: Value) {
        match &mut self.current {
            CurrentReportingContext::Object(object) | CurrentReportingContext::Capture(object) => {
                let prev = object.insert(key, ReportingValue::Value(val));
                if !cfg!(feature = "report-allow-override") {
                    assert!(prev.is_none());
                }
            }
            CurrentReportingContext::Collection(_) => {
                panic!("Cannot report value on collection");
            }
            CurrentReportingContext::Throwaway => (),
        }
    }

    fn pop_context(&mut self) {
        if matches!(self.current, CurrentReportingContext::Throwaway) {
            return;
        }
        let parent = self.context_stack.pop().expect("tried to pop from empty context");

        match parent {
            ContextStackItem::Key(key) => {
                let parent = self.context_stack.pop().expect("tried to pop from empty context");

                if let ContextStackItem::Object(mut object) = parent {
                    let mut prev_current = CurrentReportingContext::Object(Default::default());
                    swap(&mut self.current, &mut prev_current);

                    let prev = match prev_current {
                        CurrentReportingContext::Object(cur_object) => object.insert(key, ReportingValue::Object(cur_object)),
                        CurrentReportingContext::Collection(collection) => object.insert(key, ReportingValue::Collection(collection)),
                        CurrentReportingContext::Throwaway => None,
                        CurrentReportingContext::Capture(_) => panic!("Inconsistent context stack"),
                    };
                    if !cfg!(feature = "report-allow-override") {
                        assert!(prev.is_none());
                    }

                    self.current = CurrentReportingContext::Object(object);
                } else {
                    panic!("Inconsistent context stack");
                }
            }
            ContextStackItem::Collection(mut collection) => {
                let mut prev_current = CurrentReportingContext::Object(Default::default());
                swap(&mut self.current, &mut prev_current);

                match prev_current {
                    CurrentReportingContext::Object(cur_object) => {
                        collection.push(ReportingValue::Object(cur_object));
                    }
                    CurrentReportingContext::Collection(_) => {
                        panic!("Cannot insert collection into collection");
                    }
                    CurrentReportingContext::Throwaway => panic!("Inconsistent context stack"),
                    CurrentReportingContext::Capture(_) => panic!("Inconsistent context stack"),
                };

                self.current = CurrentReportingContext::Collection(collection);
            }
            _ => panic!("Inconsistent context stack"),
        }
    }

    fn unblock_or_drop_capture(&mut self) {
        if !matches!(self.current, CurrentReportingContext::Throwaway | CurrentReportingContext::Capture(_)) {
            panic!("Inconsistent context stack");
        }
        match self.context_stack.pop().expect("tried to pop from empty context") {
            ContextStackItem::Key(_) => panic!("Inconsistent context stack"),
            ContextStackItem::Collection(collection) => {
                self.current = CurrentReportingContext::Collection(collection);
            }
            ContextStackItem::Object(object) => {
                self.current = CurrentReportingContext::Object(object);
            }
            ContextStackItem::Capture(object) => {
                self.current = CurrentReportingContext::Capture(object);
            }
            ContextStackItem::Throwaway => {
                self.current = CurrentReportingContext::Throwaway;
            }
        }
    }
}

thread_local! {
    static REPORTER: RefCell<Option<Reporter>> = RefCell::new(None);
}

#[must_use]
pub struct ContextGuard(());

impl Drop for ContextGuard {
    fn drop(&mut self) {
        REPORTER.with(|reporter| reporter.borrow_mut().as_mut().map(Reporter::pop_context));
    }
}

pub fn push_context(key: &'static str) -> ContextGuard {
    REPORTER.with(|reporter| reporter.borrow_mut().as_mut().map(|r| r.create_object_under_key(key)));
    ContextGuard(())
}

#[must_use]
pub struct CollectionContextGuard(());

impl Drop for CollectionContextGuard {
    fn drop(&mut self) {
        REPORTER.with(|reporter| reporter.borrow_mut().as_mut().map(Reporter::pop_context));
    }
}

pub fn push_collection_context(key: &'static str) -> CollectionContextGuard {
    REPORTER.with(|reporter| reporter.borrow_mut().as_mut().map(|r| r.create_collection_under_key(key)));
    CollectionContextGuard(())
}

impl CollectionContextGuard {
    pub fn push_collection_item(&mut self) -> CollectionItemContextGuard {
        REPORTER.with(|reporter| reporter.borrow_mut().as_mut().map(Reporter::create_collection_item));
        CollectionItemContextGuard(self)
    }
}

#[must_use]
pub struct CollectionItemContextGuard<'a>(&'a CollectionContextGuard);

impl<'a> Drop for CollectionItemContextGuard<'a> {
    fn drop(&mut self) {
        REPORTER.with(|reporter| reporter.borrow_mut().as_mut().map(Reporter::pop_context));
    }
}

#[must_use]
pub struct BlockedReportingContextGuard();

impl Drop for BlockedReportingContextGuard {
    fn drop(&mut self) {
        REPORTER.with(|reporter| reporter.borrow_mut().as_mut().map(Reporter::unblock_or_drop_capture));
    }
}

pub fn block_reporting() -> BlockedReportingContextGuard {
    REPORTER.with(|reporter| reporter.borrow_mut().as_mut().map(Reporter::block_reporting));
    BlockedReportingContextGuard()
}

pub fn without_reporting<O>(f: impl FnOnce() -> O) -> O {
    let _blocked = block_reporting();
    f()
}

#[must_use]
pub struct CaptureReportingContextGuard();

impl Drop for CaptureReportingContextGuard {
    fn drop(&mut self) {
        REPORTER.with(|reporter| reporter.borrow_mut().as_mut().map(Reporter::unblock_or_drop_capture));
    }
}

pub fn capture_reporting() -> CaptureReportingContextGuard {
    REPORTER.with(|reporter| reporter.borrow_mut().as_mut().map(Reporter::capture_reporting));
    CaptureReportingContextGuard()
}

impl CaptureReportingContextGuard {
    pub fn reported(self) -> Option<BTreeMap<&'static str, ReportingValue>> {
        REPORTER.with(|reporter| {
            if let Some(reporter) = reporter.borrow_mut().as_mut() {
                if let CurrentReportingContext::Capture(obj) = &mut reporter.current {
                    let mut tmp = BTreeMap::new();
                    std::mem::swap(obj, &mut tmp);
                    return Some(tmp);
                } else {
                    panic!("Inconsistent context stack");
                }
            }
            None
        })
    }
}

pub fn report(key: &'static str, val: Value) {
    if cfg!(feature = "report-to-stderr") {
        eprintln!("{}: {}", key, val);
    }
    report_silent(key, val)
}

#[inline(never)]
pub fn report_silent(key: &'static str, val: Value) {
    REPORTER.with(|reporter| reporter.borrow_mut().as_mut().map(|r| r.report(key, val)));
}

#[must_use]
pub struct ReportingGuard(());

impl Drop for ReportingGuard {
    fn drop(&mut self) {
        REPORTER.with(|reporter| {
            if let Some(r) = reporter.borrow_mut().as_mut() {
                assert!(r.context_stack.is_empty());
                let mut current = CurrentReportingContext::Object(Default::default());
                swap(&mut current, &mut r.current);
                if let CurrentReportingContext::Object(object) = current {
                    println!("{}", serde_json::to_string(&object).unwrap());
                } else {
                    panic!("broken root object for reporting");
                }
            };
        });
    }
}

#[macro_export]
macro_rules! report {
    ($k:expr, $($json:tt)+) => { report($k, json!($($json)+)) };
}

#[macro_export]
macro_rules! report_silent {
    ($k:expr, $($json:tt)+) => { report_silent($k, json!($($json)+)) };
}

pub fn enable_reporting(program: &str) -> ReportingGuard {
    REPORTER.with(|reporter| reporter.replace(Some(Reporter::default())));

    report!("git_revision", built_info::GIT_VERSION.unwrap_or(""));
    report!("build_target", built_info::TARGET);
    report!("build_profile", built_info::PROFILE);
    report!("feature_flags", built_info::FEATURES_STR);
    report!("build_time", built_info::BUILT_TIME_UTC);
    report!("build_with_rustc", built_info::RUSTC_VERSION);

    if let Ok(hostname) = std::process::Command::new("hostname").output() {
        report!("hostname", String::from_utf8(hostname.stdout).unwrap().trim());
    }

    report!("program", program);
    report!("start_time", chrono::prelude::Utc::now().to_rfc3339());
    report!("args", std::env::args().collect::<Vec<String>>());

    ReportingGuard(())
}

pub mod benchmark;
pub use benchmark::*;
