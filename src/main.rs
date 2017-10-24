#![feature(plugin)]
#![plugin(rocket_codegen)]

extern crate rocket;
use std::path::{Path, PathBuf};
use rocket::response::NamedFile;

#[get("/")]
fn hello() -> Option<NamedFile> {
    NamedFile::open(Path::new("static/index.html")).ok()
}

#[get("/<file..>")]
fn files(file: PathBuf) -> Option<NamedFile> {
    NamedFile::open(Path::new("static/").join(file)).ok()
}

fn main() {
    let config = rocket::config::Config::build(rocket::config::Environment::Staging)
        .port(8888)
        .finalize().expect("Could not create config");

    rocket::custom(config, false)
        .mount("/", routes![hello, files])
        .launch();
}
