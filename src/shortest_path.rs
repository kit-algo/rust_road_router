use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::cmp::min;
use std::sync::mpsc::{channel, Sender, Receiver};
use std::thread;
use std::ops::{Index};
use super::graph::*;

#[derive(Debug)]
struct Query {
    from: NodeId,
    to: NodeId
}

#[derive(Debug, Clone)]
enum QueryProgress {
    Progress(State),
    Done(Option<Weight>),
}


#[derive(Copy, Clone, Eq, PartialEq, Debug)]
struct State {
    cost: Weight,
    position: NodeId,
}

// The priority queue depends on `Ord`.
// Explicitly implement the trait so the queue becomes a min-heap
// instead of a max-heap.
impl Ord for State {
    fn cmp(&self, other: &State) -> Ordering {
        // Notice that the we flip the ordering on costs.
        // In case of a tie we compare positions - this step is necessary
        // to make implementations of `PartialEq` and `Ord` consistent.
        other.cost.cmp(&self.cost)
            .then_with(|| self.position.cmp(&other.position))
    }
}

// `PartialOrd` needs to be implemented as well.
impl PartialOrd for State {
    fn partial_cmp(&self, other: &State) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug)]
struct TimestampedVector<T: Copy> {
    data: Vec<T>,
    // choose something small, as overflows are not a problem
    current: u8,
    timestamps: Vec<u8>,
    default: T
}

impl<T: Copy> TimestampedVector<T> {
    fn new(size: usize, default: T) -> TimestampedVector<T> {
        TimestampedVector {
            data: vec![default; size],
            current: 0,
            timestamps: vec![0; size],
            default: default
        }
    }

    fn reset(&mut self) {
        self.current += 1;
    }

    fn set(&mut self, index: usize, value: T) {
        self.timestamps[index] = self.current;
        self.data[index] = value;
    }
}

impl<T: Copy> Index<usize> for TimestampedVector<T> {
    type Output = T;

    fn index(&self, index: usize) -> &T {
        if self.timestamps[index] == self.current {
            &self.data[index]
        } else {
            &self.default
        }
    }
}

#[derive(Debug)]
struct SteppedDijkstra {
    graph: Graph,
    distances: TimestampedVector<Weight>,
    heap: BinaryHeap<State>,
    query: Option<Query>,
    result: Option<Option<Weight>>
}

impl SteppedDijkstra {
    fn new(graph: Graph) -> SteppedDijkstra {
        let n = graph.num_nodes();

        SteppedDijkstra {
            graph,
            // initialize tentative distances to INFINITY
            distances: TimestampedVector::new(n, INFINITY),
            // priority queue
            heap: BinaryHeap::new(),
            // the current query
            query: None,
            // the progress of the current query
            result: None
        }
    }

    fn initialize_query(&mut self, query: Query) {
        let from = query.from;
        // initialize
        self.query = Some(query);
        self.result = None;
        self.heap.clear();
        self.distances.reset();

        // Starte with origin
        self.distances.set(from as usize, 0);
        self.heap.push(State { cost: 0, position: from });
    }

    fn next_step(&mut self) -> QueryProgress {
        match self.result {
            Some(result) => QueryProgress::Done(result),
            None => {
                self.settle_next_node()
            }
        }
    }

    fn settle_next_node(&mut self) -> QueryProgress {
        let to = self.query.as_ref().expect("query was not initialized properly").to;

        // Examine the frontier with lower cost nodes first (min-heap)
        if let Some(State { cost, position }) = self.heap.pop() {
            // Alternatively we could have continued to find all shortest paths
            if position == to {
                self.result = Some(Some(cost));
                return QueryProgress::Done(Some(cost));
            }

            // Important as we may have already found a better way
            if cost <= self.distances[position as usize] {
                // For each node we can reach, see if we can find a way with
                // a lower cost going through this node
                for edge in self.graph.neighbor_iter(position) {
                    let next = State { cost: cost + edge.cost, position: edge.node };

                    // If so, add it to the frontier and continue
                    if next.cost < self.distances[next.position as usize] {
                        // Relaxation, we have now found a better way
                        self.distances.set(next.position as usize, next.cost);
                        self.heap.push(next);
                    }
                }
            }

            QueryProgress::Progress(State { cost, position })
        } else {
            self.result = Some(None);
            QueryProgress::Done(None)
        }
    }
}

#[derive(Debug)]
pub struct ShortestPathServer {
    dijkstra: SteppedDijkstra,
}

impl ShortestPathServer {
    pub fn new(graph: Graph) -> ShortestPathServer {
        ShortestPathServer {
            dijkstra: SteppedDijkstra::new(graph)
        }
    }

    pub fn distance(&mut self, from: NodeId, to: NodeId) -> Option<Weight> {
        self.dijkstra.initialize_query(Query { from, to });

        loop {
            match self.dijkstra.next_step() {
                QueryProgress::Progress(_) => continue,
                QueryProgress::Done(result) => return result
            }
        }
    }
}

#[derive(Debug)]
pub struct ShortestPathServerBiDirDijk {
    forward_dijkstra: SteppedDijkstra,
    backward_dijkstra: SteppedDijkstra,
    forward_distances: TimestampedVector<Weight>,
    backward_distances: TimestampedVector<Weight>,
    tentative_distance: Weight
}

impl ShortestPathServerBiDirDijk {
    pub fn new(graph: Graph) -> ShortestPathServerBiDirDijk {
        let n = graph.num_nodes();
        let reversed = graph.reverse();

        ShortestPathServerBiDirDijk {
            forward_dijkstra: SteppedDijkstra::new(graph),
            backward_dijkstra: SteppedDijkstra::new(reversed),
            forward_distances: TimestampedVector::new(n, INFINITY),
            backward_distances: TimestampedVector::new(n, INFINITY),
            tentative_distance: INFINITY
        }
    }

    pub fn distance(&mut self, from: NodeId, to: NodeId) -> Option<Weight> {
        // initialize
        self.forward_distances.reset();
        self.backward_distances.reset();
        self.tentative_distance = INFINITY;

        self.forward_dijkstra.initialize_query(Query { from, to });
        self.backward_dijkstra.initialize_query(Query { from: to, to: from });

        // Starte with origin
        self.forward_distances.set(from as usize, 0);
        self.backward_distances.set(to as usize, 0);

        let mut forward_progress = 0;
        let mut backward_progress = 0;

        while self.tentative_distance > forward_progress + backward_progress {
            if forward_progress <= backward_progress {
                match self.forward_dijkstra.next_step() {
                    QueryProgress::Progress(State { cost, position }) => {
                        forward_progress = cost;
                        self.forward_distances.set(position as usize, cost);
                        self.tentative_distance = min(cost + self.backward_distances[position as usize], self.tentative_distance);
                    },
                    QueryProgress::Done(result) => return result
                }
            } else {
                match self.backward_dijkstra.next_step() {
                    QueryProgress::Progress(State { cost, position }) => {
                        backward_progress = cost;
                        self.backward_distances.set(position as usize, cost);
                        self.tentative_distance = min(cost + self.forward_distances[position as usize], self.tentative_distance);
                    },
                    QueryProgress::Done(result) => return result
                }
            }
        }

        match self.tentative_distance {
            INFINITY => None,
            dist => Some(dist)
        }
    }
}

#[derive(Debug)]
enum ServerControl {
    Query(Query),
    Break,
    Shutdown
}

#[derive(Debug)]
pub struct AsyncShortestPathServer {
    query_sender: Sender<ServerControl>,
    progress_receiver: Receiver<QueryProgress>
}

impl AsyncShortestPathServer {
    pub fn new(graph: Graph) -> AsyncShortestPathServer {
        let (query_sender, query_receiver) = channel();
        let (progress_sender, progress_receiver) = channel();

        thread::spawn(move || {
            let mut dijkstra = SteppedDijkstra::new(graph);

            loop {
                match query_receiver.recv() {
                    Ok(ServerControl::Query(query)) => {
                        dijkstra.initialize_query(query);

                        loop {
                            match dijkstra.next_step() {
                                QueryProgress::Progress(_) => (),
                                progress @ QueryProgress::Done(_) => {
                                    progress_sender.send(progress).unwrap();
                                    break
                                }
                            }
                        }
                    },
                    Ok(ServerControl::Break) => (),
                    Ok(ServerControl::Shutdown) | Err(_) => break
                }
            }
        });

        AsyncShortestPathServer {
            query_sender,
            progress_receiver
        }
    }

    pub fn distance(&self, from: NodeId, to: NodeId) -> Option<Weight> {
        self.query_sender.send(ServerControl::Query(Query { from, to })).unwrap();
        loop {
            match self.progress_receiver.recv() {
                Ok(QueryProgress::Done(result)) => return result,
                Ok(QueryProgress::Progress(_)) => continue,
                Err(e) => panic!("{:?}", e)
            }
        }
    }
}

impl Drop for AsyncShortestPathServer {
    fn drop(&mut self) {
        self.query_sender.send(ServerControl::Shutdown).unwrap();
    }
}







#[derive(Debug)]
pub struct AsyncShortestPathServerBiDirDijk {
    forward_query_sender: Sender<(ServerControl, u32)>,
    backward_query_sender: Sender<(ServerControl, u32)>,
    forward_progress_receiver: Receiver<(QueryProgress, u32)>,
    backward_progress_receiver: Receiver<(QueryProgress, u32)>,

    forward_distances: TimestampedVector<Weight>,
    backward_distances: TimestampedVector<Weight>,
    tentative_distance: Weight,

    active_query_id: u32
}

impl AsyncShortestPathServerBiDirDijk {
    pub fn new(graph: Graph) -> AsyncShortestPathServerBiDirDijk {
        let (forward_query_sender, forward_query_receiver) = channel();
        let (forward_progress_sender, forward_progress_receiver) = channel();
        let (backward_query_sender, backward_query_receiver) = channel();
        let (backward_progress_sender, backward_progress_receiver) = channel();

        let n = graph.num_nodes();
        let reversed = graph.reverse();

        thread::spawn(move || {
            let mut dijkstra = SteppedDijkstra::new(graph);

            loop {
                match forward_query_receiver.recv() {
                    Ok((ServerControl::Query(query), active_query_id)) => {
                        dijkstra.initialize_query(query);

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
            let mut dijkstra = SteppedDijkstra::new(reversed);

            loop {
                match backward_query_receiver.recv() {
                    Ok((ServerControl::Query(query), active_query_id)) => {
                        dijkstra.initialize_query(query);

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

        AsyncShortestPathServerBiDirDijk {
            forward_query_sender,
            backward_query_sender,
            forward_progress_receiver,
            backward_progress_receiver,

            forward_distances: TimestampedVector::new(n, INFINITY),
            backward_distances: TimestampedVector::new(n, INFINITY),
            tentative_distance: INFINITY,

            active_query_id: 0
        }
    }

    pub fn distance(&mut self, from: NodeId, to: NodeId) -> Option<Weight> {
        // initialize
        self.forward_distances.reset();
        self.backward_distances.reset();
        self.tentative_distance = INFINITY;
        self.active_query_id += 1;

        self.forward_query_sender.send((ServerControl::Query(Query { from, to }), self.active_query_id)).unwrap();
        self.backward_query_sender.send((ServerControl::Query(Query { from: to, to: from }), self.active_query_id)).unwrap();

        // Starte with origin
        self.forward_distances.set(from as usize, 0);
        self.backward_distances.set(to as usize, 0);

        let mut forward_progress = 0;
        let mut backward_progress = 0;

        while self.tentative_distance > forward_progress + backward_progress {
            match self.forward_progress_receiver.recv() {
                Ok((_, query_id)) if query_id != self.active_query_id => (),
                Ok((QueryProgress::Done(result), _)) => {
                    self.backward_query_sender.send((ServerControl::Break, self.active_query_id)).unwrap();
                    return result
                },
                Ok((QueryProgress::Progress(State { cost, position }), _)) => {
                    forward_progress = cost;
                    self.forward_distances.set(position as usize, cost);
                    self.tentative_distance = min(cost + self.backward_distances[position as usize], self.tentative_distance);
                },
                Err(e) => panic!("{:?}", e)
            }
            match self.backward_progress_receiver.recv() {
                Ok((_, query_id)) if query_id != self.active_query_id => (),
                Ok((QueryProgress::Done(result), _)) => {
                    self.forward_query_sender.send((ServerControl::Break, self.active_query_id)).unwrap();
                    return result
                },
                Ok((QueryProgress::Progress(State { cost, position }), _)) => {
                    backward_progress = cost;
                    self.backward_distances.set(position as usize, cost);
                    self.tentative_distance = min(cost + self.forward_distances[position as usize], self.tentative_distance);
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

impl Drop for AsyncShortestPathServerBiDirDijk {
    fn drop(&mut self) {
        self.forward_query_sender.send((ServerControl::Shutdown, 0)).unwrap();
        self.backward_query_sender.send((ServerControl::Shutdown, 0)).unwrap();
    }
}
