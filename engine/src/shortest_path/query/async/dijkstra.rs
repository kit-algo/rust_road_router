use super::*;

#[derive(Debug)]
pub struct Server {
    query_sender: Sender<ServerControl>,
    progress_receiver: Receiver<QueryProgress>
}

impl Server {
    pub fn new(graph: Graph) -> Server {
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

        Server {
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

impl Drop for Server {
    fn drop(&mut self) {
        self.query_sender.send(ServerControl::Shutdown).unwrap();
    }
}
