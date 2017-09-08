extern crate bmw_routing_engine;
extern crate svg;

use std::process::Command;
use std::env;
use std::path::Path;

use bmw_routing_engine::*;
use io::read_into_vector;

use svg::Document;
use svg::node::element::Line;

fn main() {
    let mut args = env::args();
    args.next();

    let arg = &args.next().expect("No directory arg given");
    let path = Path::new(arg);

    let _first_out = read_into_vector(path.join("first_out").to_str().unwrap()).expect("could not read first_out");
    let _head = read_into_vector(path.join("head").to_str().unwrap()).expect("could not read head");
    let _travel_time = read_into_vector(path.join("travel_time").to_str().unwrap()).expect("could not read travel_time");
    let _ch_order = read_into_vector(path.join("travel_time_ch/order").to_str().unwrap()).expect("could not read travel_time_ch/order");

    let output = &args.next().unwrap_or("image.svg".to_owned());

    let line = Line::new()
        .set("x1", "0")
        .set("y1", "0")
        .set("x2", "100")
        .set("y2", "100")
        .set("class", "edge");

    let document = Document::new()
        .set("viewBox", (0, 0, 800, 600))
        .add(line);

    svg::save(output, &document).unwrap();

    Command::new("sh")
        .arg("-c")
        .arg("echo '<?xml-stylesheet type=\"text/css\" href=\"style.css\"?>' > tmp")
        .output()
        .expect("failed to create tmp");

    Command::new("sh")
        .arg("-c")
        .arg(format!("cat {} >> tmp", output))
        .output()
        .expect("failed to cat");

    Command::new("sh")
        .arg("-c")
        .arg(format!("mv tmp {}", output))
        .output()
        .expect("failed to mv");
}
