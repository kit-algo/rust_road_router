use super::*;
use csv::ReaderBuilder;
use glob::glob;
use std::path::Path;
use std::fs::File;

#[derive(Debug)]
pub struct CSVSource<'a> {
    directory: &'a Path
}

impl<'a> CSVSource<'a> {
    pub fn new(directory: &'a Path) -> CSVSource<'a> {
        CSVSource { directory }
    }
}

impl<'a> RdfDataSource for CSVSource<'a> {
    fn links(&self) -> Vec<RdfLink> {
        let mut elements = vec![];

        for entry in glob(self.directory.join("rdf_link/rdf_link.txt*").to_str().unwrap()).unwrap() {
            match entry {
                Ok(path) => {
                    let file = File::open(path.clone()).unwrap();
                    let mut reader = ReaderBuilder::new()
                        .has_headers(false)
                        .delimiter(b'\t')
                        .quoting(false)
                        .double_quote(false)
                        .escape(None)
                        .from_reader(file);

                    for (i, line) in reader.records().enumerate() {
                        let record = line.unwrap();

                        elements.push(RdfLink {
                            link_id: record[0].parse().unwrap_or_else(|_| panic!("could not parse {:?} as link_id in line {} of {:?}", &record[0], i, path)),
                            ref_node_id: record[1].parse().unwrap_or_else(|_| panic!("could not parse {:?} as ref_node_id in line {} of {:?}", &record[1], i, path)),
                            nonref_node_id: record[2].parse().unwrap_or_else(|_| panic!("could not parse {:?} as nonref_node_id in line {} of {:?}", &record[2], i, path))
                        });
                    }
                },
                Err(e) => println!("{:?}", e),
            }
        }

        elements
    }

    fn nav_links(&self) -> Vec<RdfNavLink> {
        let mut elements = vec![];

        for entry in glob(self.directory.join("rdf_nav_link/rdf_nav_link.txt*").to_str().unwrap()).unwrap() {
            match entry {
                Ok(path) => {
                    let file = File::open(path.clone()).unwrap();
                    let mut reader = ReaderBuilder::new()
                        .has_headers(false)
                        .delimiter(b'\t')
                        .quoting(false)
                        .double_quote(false)
                        .escape(None)
                        .from_reader(file);

                    for (i, line) in reader.records().enumerate() {
                        let record = line.unwrap();

                        elements.push(RdfNavLink {
                            link_id: record[0].parse().unwrap_or_else(|_| panic!("could not parse {:?} as link_id in line {} of {:?}", &record[0], i, path)),
                            travel_direction: record[6].parse().unwrap_or_else(|_| panic!("could not parse {:?} as travel_direction in line {} of {:?}", &record[6], i, path)),
                            speed_category: record[19].parse().unwrap_or_else(|_| panic!("could not parse {:?} as speed_category in line {} of {:?}", &record[19], i, path)),
                            from_ref_speed_limit: record[25].parse().ok(),
                            to_ref_speed_limit: record[26].parse().ok()
                        });
                    }
                },
                Err(e) => println!("{:?}", e),
            }
        }

        elements
    }


    fn nodes(&self) -> Vec<RdfNode> {
        let mut elements = vec![];

        for entry in glob(self.directory.join("rdf_node/rdf_node.txt*").to_str().unwrap()).unwrap() {
            match entry {
                Ok(path) => {
                    let file = File::open(path.clone()).unwrap();
                    let mut reader = ReaderBuilder::new()
                        .has_headers(false)
                        .delimiter(b'\t')
                        .quoting(false)
                        .double_quote(false)
                        .escape(None)
                        .from_reader(file);

                    for (i, line) in reader.records().enumerate() {
                        let record = line.unwrap();

                        elements.push(RdfNode {
                            node_id: record[0].parse().unwrap_or_else(|_| panic!("could not parse {:?} as node_id in line {} of {:?}", &record[0], i, path)),
                            lat: record[1].parse().unwrap_or_else(|_| panic!("could not parse {:?} as lat in line {} of {:?}", &record[1], i, path)),
                            lon: record[2].parse().unwrap_or_else(|_| panic!("could not parse {:?} as lon in line {} of {:?}", &record[2], i, path)),
                            z_coord: record[3].parse().ok(),
                        });
                    }
                },
                Err(e) => println!("{:?}", e),
            }
        }

        elements
    }

    fn link_geometries(&self) -> Vec<RdfLinkGeometry> {
        let mut elements = vec![];

        for entry in glob(self.directory.join("adas_link_geometry/adas_link_geometry.txt*").to_str().unwrap()).unwrap() {
            match entry {
                Ok(path) => {
                    let file = File::open(path.clone()).unwrap();
                    let mut reader = ReaderBuilder::new()
                        .has_headers(false)
                        .delimiter(b'\t')
                        .quoting(false)
                        .double_quote(false)
                        .escape(None)
                        .from_reader(file);

                    for (i, line) in reader.records().enumerate() {
                        let record = line.unwrap();

                        elements.push(RdfLinkGeometry {
                            link_id: record[0].parse().unwrap_or_else(|_| panic!("could not parse {:?} as link_id in line {} of {:?}", &record[0], i, path)),
                            seq_num: record[1].parse().unwrap_or_else(|_| panic!("could not parse {:?} as seq_num in line {} of {:?}", &record[1], i, path)),
                            lat: record[2].parse().unwrap_or_else(|_| panic!("could not parse {:?} as lat in line {} of {:?}", &record[2], i, path)),
                            lon: record[3].parse().unwrap_or_else(|_| panic!("could not parse {:?} as lon in line {} of {:?}", &record[3], i, path)),
                            z_coord: record[4].parse().ok(),
                        });
                    }
                },
                Err(e) => println!("{:?}", e),
            }
        }

        elements
    }
}
