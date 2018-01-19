use time;

pub fn measure<F: FnMut()>(name: &str, mut f: F) {
    let start = time::now();
    f();
    println!("{}: {}ms", name, (time::now() - start).num_milliseconds());
}
