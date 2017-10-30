use std::sync::mpsc::{channel, Sender, Receiver};
use std::thread;
use super::*;

pub mod dijkstra;
pub mod bidirectional_dijkstra;

#[derive(Debug)]
enum ServerControl {
    Query(Query),
    Break,
    Shutdown
}
