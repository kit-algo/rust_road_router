use super::*;

#[derive(Debug)]
pub enum Event<'a> {
    Link(&'a LinkData),
    Trace(&'a TraceData),
}

#[derive(Debug)]
enum IteratorToPoll {
    Links,
    Traces,
}

pub struct EventIterator<'a> {
    links: Box<dyn Iterator<Item = &'a LinkData> + 'a>,
    traces: Box<dyn Iterator<Item = &'a TraceData> + 'a>,
    iterator_to_poll: IteratorToPoll,
    next_link: Option<&'a LinkData>,
    next_trace: &'a TraceData,
    done: bool,
}

impl<'a> EventIterator<'a> {
    pub fn new(mut links: Box<dyn Iterator<Item = &'a LinkData> + 'a>, mut traces: Box<dyn Iterator<Item = &'a TraceData> + 'a>) -> Result<EventIterator<'a>, &'static str> {
        let next_link = links.next().ok_or("no links given")?;
        let next_trace = traces.next().ok_or("no traces given")?;

        Ok(EventIterator {
            links,
            traces,
            iterator_to_poll: IteratorToPoll::Links,
            next_link: Some(next_link),
            next_trace,
            done: false
        })
    }
}

impl<'a> Iterator for EventIterator<'a> {
    type Item = Event<'a>;

    fn next(&mut self) -> Option<Event<'a>> {
        if self.done { return None }

        match self.iterator_to_poll {
            IteratorToPoll::Links => {
                let result = Some(Event::Link(self.next_link.unwrap()));

                if self.next_link.unwrap().link_id == self.next_trace.link_id {
                    self.iterator_to_poll = IteratorToPoll::Traces;
                } else {
                    self.iterator_to_poll = IteratorToPoll::Links;
                }

                self.next_link = self.links.next();

                result
            },
            IteratorToPoll::Traces => {
                let result = Some(Event::Trace(self.next_trace));

                match self.traces.next() {
                    Some(trace) => {
                        if self.next_trace.link_id == trace.link_id {
                            self.iterator_to_poll = IteratorToPoll::Traces;
                        } else {
                            self.iterator_to_poll = IteratorToPoll::Links;
                        }

                        self.next_trace = trace;
                    },
                    None => {
                        debug_assert_eq!(self.links.next(), None);
                        self.done = true;
                    },
                }

                result
            },
        }
    }
}
