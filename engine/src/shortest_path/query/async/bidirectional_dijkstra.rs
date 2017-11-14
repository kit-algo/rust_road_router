use super::*;
use std::cmp::min;
use std::ptr;
use std::sync::{Arc, Barrier};
use std::sync::atomic::compiler_fence;
use std::sync::atomic::Ordering::SeqCst;
use std::ops::Deref;

use shortest_path::timestamped_vector::TimestampedVector;

#[derive(Debug)]
struct DistancesPointerWrapper {
    pointer: *mut *const TimestampedVector<Weight>
}
unsafe impl Send for DistancesPointerWrapper {}

#[derive(Debug)]
pub struct Server {
    forward_query_sender: Sender<(ServerControl, u32)>,
    backward_query_sender: Sender<(ServerControl, u32)>,
    forward_progress_receiver: Receiver<(QueryProgress, u32)>,
    backward_progress_receiver: Receiver<(QueryProgress, u32)>,

    forward_distances_box: Box<*const TimestampedVector<Weight>>,
    backward_distances_box: Box<*const TimestampedVector<Weight>>,
    tentative_distance: Weight,

    active_query_id: u32
}

impl Server {
    pub fn new<C: 'static + Send + Deref<Target = Graph>>(graph: C) -> Server {
        let (forward_query_sender, forward_query_receiver) = channel();
        let (forward_progress_sender, forward_progress_receiver) = channel();
        let (backward_query_sender, backward_query_receiver) = channel();
        let (backward_progress_sender, backward_progress_receiver) = channel();

        let forward_distances_box = Box::new(ptr::null() as *const TimestampedVector<Weight>);
        let forward_distances_pointer = DistancesPointerWrapper { pointer: Box::into_raw(forward_distances_box) };
        let forward_distances_box = unsafe { Box::from_raw(forward_distances_pointer.pointer) };

        let backward_distances_box = Box::new(ptr::null() as *const TimestampedVector<Weight>);
        let backward_distances_pointer = DistancesPointerWrapper { pointer: Box::into_raw(backward_distances_box) };
        let backward_distances_box = unsafe { Box::from_raw(backward_distances_pointer.pointer) };

        let reversed = graph.reverse();

        let forward_query_barrier = Arc::new(Barrier::new(2));
        let backward_query_barrier = forward_query_barrier.clone();

        thread::spawn(move || {
            let mut dijkstra = SteppedDijkstra::new(graph);

            unsafe { *forward_distances_pointer.pointer = dijkstra.distances_pointer() };
            compiler_fence(SeqCst);

            loop {
                match forward_query_receiver.recv() {
                    Ok((ServerControl::Query(query), active_query_id)) => {
                        dijkstra.initialize_query(query);

                        forward_query_barrier.wait();

                        loop {
                            match forward_query_receiver.try_recv() {
                                Ok((ServerControl::Break, query_id)) if active_query_id == query_id => break,
                                Ok((ServerControl::Query(_), query_id)) => panic!("forward received new query {} while still processing {}", query_id, active_query_id),
                                _ => ()
                            }

                            let progress = dijkstra.next_step();
                            forward_progress_sender.send((progress.clone(), active_query_id)).unwrap();
                            if let QueryProgress::Done(_) = progress { break }
                        }
                    },
                    Ok((ServerControl::Break, _)) => (),
                    Ok((ServerControl::Shutdown, _)) | Err(_) => break
                }
            }
        });

        thread::spawn(move || {
            let mut dijkstra = SteppedDijkstra::new(Box::new(reversed));

            unsafe { *backward_distances_pointer.pointer = dijkstra.distances_pointer() };
            compiler_fence(SeqCst);

            loop {
                match backward_query_receiver.recv() {
                    Ok((ServerControl::Query(query), active_query_id)) => {
                        dijkstra.initialize_query(query);

                        backward_query_barrier.wait();

                        loop {
                            match backward_query_receiver.try_recv() {
                                Ok((ServerControl::Break, query_id)) if active_query_id == query_id => break,
                                Ok((ServerControl::Query(_), query_id)) => panic!("backward received new query {} while still processing {}", query_id, active_query_id),
                                _ => ()
                            }

                            let progress = dijkstra.next_step();
                            backward_progress_sender.send((progress.clone(), active_query_id)).unwrap();
                            if let QueryProgress::Done(_) = progress { break }
                        }
                    },
                    Ok((ServerControl::Break, _)) => (),
                    Ok((ServerControl::Shutdown, _)) | Err(_) => break
                }
            }
        });

        Server {
            forward_query_sender,
            backward_query_sender,
            forward_progress_receiver,
            backward_progress_receiver,

            forward_distances_box,
            backward_distances_box,
            tentative_distance: INFINITY,

            active_query_id: 0
        }
    }

    pub fn distance(&mut self, from: NodeId, to: NodeId) -> Option<Weight> {
        // initialize
        self.tentative_distance = INFINITY;
        self.active_query_id += 1;

        self.forward_query_sender.send((ServerControl::Query(Query { from, to }), self.active_query_id)).unwrap();
        self.backward_query_sender.send((ServerControl::Query(Query { from: to, to: from }), self.active_query_id)).unwrap();

        let mut forward_progress = 0;
        let mut backward_progress = 0;

        while self.tentative_distance >= forward_progress + backward_progress {
            // some sort of select would be nice to avoid waiting on one direction while there is data available from the other one
            // there is a select! macro, but the API is marked as unstable, so I'm not going to use it here
            // https://github.com/rust-lang/rust/issues/27800
            match self.forward_progress_receiver.recv() {
                Ok((_, query_id)) if query_id != self.active_query_id => (),
                Ok((QueryProgress::Done(result), _)) => {
                    self.backward_query_sender.send((ServerControl::Break, self.active_query_id)).unwrap();
                    return result
                },
                Ok((QueryProgress::Progress(State { distance, node }), _)) => {
                    forward_progress = distance;
                    let other_distance = unsafe { (**self.backward_distances_box)[node as usize] };
                    self.tentative_distance = min(distance + other_distance, self.tentative_distance);
                },
                Err(e) => panic!("{:?}", e)
            }
            match self.backward_progress_receiver.recv() {
                Ok((_, query_id)) if query_id != self.active_query_id => (),
                Ok((QueryProgress::Done(result), _)) => {
                    self.forward_query_sender.send((ServerControl::Break, self.active_query_id)).unwrap();
                    return result
                },
                Ok((QueryProgress::Progress(State { distance, node }), _)) => {
                    backward_progress = distance;
                    let other_distance = unsafe { (**self.forward_distances_box)[node as usize] };
                    self.tentative_distance = min(distance + other_distance, self.tentative_distance);
                },
                Err(e) => panic!("{:?}", e)
            }
        }

        self.forward_query_sender.send((ServerControl::Break, self.active_query_id)).unwrap();
        self.backward_query_sender.send((ServerControl::Break, self.active_query_id)).unwrap();

        match self.tentative_distance {
            INFINITY => None,
            dist => Some(dist)
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.forward_query_sender.send((ServerControl::Shutdown, 0)).unwrap();
        self.backward_query_sender.send((ServerControl::Shutdown, 0)).unwrap();
    }
}
