use std::env;

extern crate bmw_routing_engine;

use bmw_routing_engine::*;
use import::here;
use import::here::postgres_source::PostgresSource;


fn main() {
    let mut args = env::args();
    args.next();

    let db_url = &args.next().expect("No database connection given");
    let postgres_source = PostgresSource::new(db_url).expect("Could not connect to postgres");
    let graph = here::read_graph(&postgres_source);
    println!("{:?}", graph);
}
