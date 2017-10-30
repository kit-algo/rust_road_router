#![feature(plugin)]
#![feature(custom_derive)]
#![plugin(rocket_codegen)]

#[macro_use]
extern crate serde_derive;

extern crate rocket;
extern crate rocket_contrib;
use std::path::{Path, PathBuf};
use rocket::response::NamedFile;
use rocket_contrib::Json;

#[derive(Debug, FromForm)]
struct RoutingQuery {
    from_lat: f32,
    from_lng: f32,
    to_lat: f32,
    to_lng: f32
}

#[derive(Debug, Serialize, Deserialize)]
struct RoutingQueryResponse {
    // TODO Weight
    distance: u32,
    path: Vec<(f32, f32)>
}

#[get("/")]
fn index() -> Option<NamedFile> {
    NamedFile::open(Path::new("static/index.html")).ok()
}

#[get("/<file..>")]
fn files(file: PathBuf) -> Option<NamedFile> {
    NamedFile::open(Path::new("static/").join(file)).ok()
}

#[get("/query?<query_params>", format = "application/json")]
fn query(query_params: RoutingQuery) -> Json<Option<RoutingQueryResponse>> {
    let RoutingQuery { from_lat, from_lng, to_lat, to_lng } = query_params;
    Json(Some(RoutingQueryResponse { distance: 0, path: vec![(from_lat, from_lng), (to_lat, to_lng)] }))
}

fn main() {
    let config = rocket::config::Config::build(rocket::config::Environment::Staging)
        .port(8888)
        .finalize().expect("Could not create config");

    rocket::custom(config, false)
        .mount("/", routes![index, files, query])
        .launch();
}
