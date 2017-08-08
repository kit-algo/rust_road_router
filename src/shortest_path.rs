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
    to: NodeId,
    run: u32
}

#[derive(Debug)]
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
        self.dijkstra.initialize_query(Query { from, to, run: 0 });

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

        self.forward_dijkstra.initialize_query(Query { from, to, run: 0 });
        self.backward_dijkstra.initialize_query(Query { from: to, to: from, run: 0 });

        // Starte with origin
        self.forward_distances.set(from as usize, 0);
        self.backward_distances.set(to as usize, 0);

        let mut forward_progress = 0;
        let mut backward_progress = 0;
        let mut forward_done = false;
        let mut backward_done = false;

        while self.tentative_distance > forward_progress + backward_progress && !(forward_done && backward_done) {
            if forward_progress <= backward_progress && !forward_done {
                match self.forward_dijkstra.next_step() {
                    QueryProgress::Progress(State { cost, position }) => {
                        forward_progress = cost;
                        self.forward_distances.set(position as usize, cost);
                        self.tentative_distance = min(cost + self.backward_distances[position as usize], self.tentative_distance);
                    },
                    QueryProgress::Done(result) => {
                        forward_done = true;
                        match result {
                            Some(_) => {
                                return result;
                            },
                            None => (),
                        }
                    }
                }
            } else {
                match self.backward_dijkstra.next_step() {
                    QueryProgress::Progress(State { cost, position }) => {
                        backward_progress = cost;
                        self.backward_distances.set(position as usize, cost);
                        self.tentative_distance = min(cost + self.forward_distances[position as usize], self.tentative_distance);
                    },
                    QueryProgress::Done(result) => {
                        backward_done = true;
                        match result {
                            Some(_) => {
                                return result;
                            },
                            None => (),
                        }
                    }
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
        self.query_sender.send(ServerControl::Query(Query { from, to, run: 0 })).unwrap();
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
