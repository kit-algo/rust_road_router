extern crate bmw_routing_engine;
extern crate svg;

use std::process::Command;

use bmw_routing_engine::*;
use graph::first_out_graph::FirstOutGraph as Graph;

use svg::Document;
use svg::node::element::Line;

fn main() {
    let line = Line::new()
        .set("x1", "0")
        .set("y1", "0")
        .set("x2", "100")
        .set("y2", "100")
        .set("stroke", "black")
        .set("stroke-width", 3);

    let document = Document::new()
        .set("viewBox", (0, 0, 800, 600))
        .add(line);

    svg::save("image.svg", &document).unwrap();

    Command::new("sh")
        .arg("-c")
        .arg("echo '<?xml-stylesheet type=\"text/css\" href=\"style.css\"?>' > tmp")
        .output()
        .expect("failed to create tmp");

    Command::new("sh")
        .arg("-c")
        .arg("cat image.svg >> tmp")
        .output()
        .expect("failed to cat");

    Command::new("sh")
        .arg("-c")
        .arg("mv tmp image.svg")
        .output()
        .expect("failed to mv");
}
