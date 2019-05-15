//! Utility module for command line interfaces

use std::{fmt, fmt::Display, error::Error};

/// An error struct to wrap simple static error messages
#[derive(Debug)]
pub struct CliErr(pub &'static str);

impl Display for CliErr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl Error for CliErr {}
