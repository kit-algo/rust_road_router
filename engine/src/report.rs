use std::{
    cell::RefCell,
    mem::swap,
};
use serde_json::{Value, Map};

pub use serde_json::json;

#[derive(Debug)]
enum ContextStackItem {
    Key(String),
    Collection(Vec<Value>),
    Object(Map<String, Value>),
}

#[derive(Debug)]
enum CurrentReportingContext {
    Collection(Vec<Value>),
    Object(Map<String, Value>),
}

#[derive(Debug)]
pub struct Reporter {
    current: CurrentReportingContext,
    context_stack: Vec<ContextStackItem>,
}

impl Default for Reporter {
    fn default() -> Self {
        Reporter {
            current: CurrentReportingContext::Object(Map::new()),
            context_stack: Vec::new(),
        }
    }
}

impl Reporter {
    fn create_object_under_key(&mut self, key: String) {
        match &mut self.current {
            CurrentReportingContext::Object(object) => {
                let mut tmp = Map::new();
                swap(&mut tmp, object);
                self.context_stack.push(ContextStackItem::Object(tmp));
                self.context_stack.push(ContextStackItem::Key(key));
            },
            CurrentReportingContext::Collection(_) => {
                panic!("Cannot create object at key in collection");
            },
        }
    }

    fn create_collection_under_key(&mut self, key: String) {
        match &mut self.current {
            CurrentReportingContext::Object(object) => {
                let mut tmp = Map::new();
                swap(&mut tmp, object);
                self.context_stack.push(ContextStackItem::Object(tmp));
                self.context_stack.push(ContextStackItem::Key(key));
                self.current = CurrentReportingContext::Collection(Vec::new());
            },
            CurrentReportingContext::Collection(_) => {
                panic!("Cannot create collection at key in collection");
            },
        }
    }

    fn create_collection_item(&mut self) {
        match &mut self.current {
            CurrentReportingContext::Object(_) => {
                panic!("Cannot create collection item in object");
            },
            CurrentReportingContext::Collection(collection) => {
                let mut tmp = Vec::new();
                swap(&mut tmp, collection);
                self.context_stack.push(ContextStackItem::Collection(tmp));
                self.current = CurrentReportingContext::Object(Map::new());
            },
        }
    }

    fn report(&mut self, key: String, val: Value) {
        match &mut self.current {
            CurrentReportingContext::Object(object) => {
                let prev = object.insert(key, val);
                assert_eq!(prev, None);
            },
            CurrentReportingContext::Collection(_) => {
                panic!("Cannot report value on collection");
            },
        }
    }

    fn pop_context(&mut self) {
        let parent = self.context_stack.pop().expect("tried to pop from empty context");

        match parent {
            ContextStackItem::Key(key) => {
                let parent = self.context_stack.pop().expect("tried to pop from empty context");

                if let ContextStackItem::Object(mut object) = parent {
                    let mut prev_current = CurrentReportingContext::Object(Default::default());
                    swap(&mut self.current, &mut prev_current);

                    let prev = match prev_current {
                        CurrentReportingContext::Object(cur_object) => {
                            object.insert(key, Value::Object(cur_object))
                        },
                        CurrentReportingContext::Collection(collection) => {
                            object.insert(key, Value::Array(collection))
                        },
                    };
                    assert_eq!(prev, None);

                    self.current = CurrentReportingContext::Object(object);
                } else {
                    panic!("Inconsistent context stack");
                }
            },
            ContextStackItem::Collection(mut collection) => {
                let mut prev_current = CurrentReportingContext::Object(Default::default());
                swap(&mut self.current, &mut prev_current);

                match prev_current {
                    CurrentReportingContext::Object(cur_object) => {
                        collection.push(Value::Object(cur_object));
                    },
                    CurrentReportingContext::Collection(_) => {
                        panic!("Cannot insert collection into collection");
                    },
                };

                self.current = CurrentReportingContext::Collection(collection);
            },
            ContextStackItem::Object(_) => {
                panic!("Inconsistent context stack");
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
        REPORTER.with(|reporter| reporter.borrow_mut().as_mut().map(|r| r.pop_context()));
    }
}

pub fn push_context(key: String) -> ContextGuard {
    REPORTER.with(|reporter| reporter.borrow_mut().as_mut().map(|r| r.create_object_under_key(key)));
    ContextGuard(())
}

#[must_use]
pub struct CollectionContextGuard(());

impl Drop for CollectionContextGuard {
    fn drop(&mut self) {
        REPORTER.with(|reporter| reporter.borrow_mut().as_mut().map(|r| r.pop_context()));
    }
}

pub fn push_collection_context(key: String) -> CollectionContextGuard {
    REPORTER.with(|reporter| reporter.borrow_mut().as_mut().map(|r| r.create_collection_under_key(key)));
    CollectionContextGuard(())
}

impl CollectionContextGuard {
    pub fn push_collection_item(&mut self) -> CollectionItemContextGuard {
        REPORTER.with(|reporter| reporter.borrow_mut().as_mut().map(|r| r.create_collection_item()));
        CollectionItemContextGuard(self)
    }
}

#[must_use]
pub struct CollectionItemContextGuard<'a>(&'a CollectionContextGuard);

impl<'a> Drop for CollectionItemContextGuard<'a> {
    fn drop(&mut self) {
        REPORTER.with(|reporter| reporter.borrow_mut().as_mut().map(|r| r.pop_context()));
    }
}

pub fn report(key: String, val: Value) {
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
                    println!("{}", Value::Object(object).to_string());
                } else {
                    panic!("broken root object for reporting");
                }
            };
        });
    }
}

pub fn enable_reporting() -> ReportingGuard {
    REPORTER.with(|reporter| reporter.replace(Some(Reporter::default())));
    ReportingGuard(())
}

#[macro_export]
macro_rules! report {
    ($k:expr, $($json:tt)+) => { report($k.to_string(), json!($($json)+)) };
}
