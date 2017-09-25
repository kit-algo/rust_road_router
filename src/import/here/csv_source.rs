use super::*;
use csv::Reader;
use glob::glob;
use std::path::Path;
use std::fs::File;

#[derive(Debug)]
struct CSVSource<'a> {
    directory: &'a Path
}

impl<'a> CSVSource<'a> {
    pub fn new(directory: &'a Path) -> CSVSource<'a> {
        CSVSource { directory }
    }
}

impl<'a> RdfDataSource for CSVSource<'a> {
    fn links(&self) -> Vec<RdfLink> {
        let elements = vec![];

        for entry in glob(self.directory.join("rdf_link.txt*").to_str().unwrap()).unwrap() {
            match entry {
                Ok(path) => {
                    let file = File::open(path).unwrap();
                    let mut reader = Reader::from_reader(file);

                    for (i, line) in reader.records().enumerate() {
                        let record = line.unwrap();

                        elements.push(RdfLink {
                            link_id: record[0].parse().expect(&format!("could not parse {:?} as link_id in line {} of {:?}", &record[0], i, path)),
                            ref_node_id: record[1].parse().expect(&format!("could not parse {:?} as ref_node_id in line {} of {:?}", &record[1], i, path)),
                            nonref_node_id: record[2].parse().expect(&format!("could not parse {:?} as nonref_node_id in line {} of {:?}", &record[2], i, path))
                        });
                    }
                },
                Err(e) => println!("{:?}", e),
            }
        }

        elements
    }

    fn nav_links(&self) -> Vec<RdfNavLink> {
        let elements = vec![];

        for entry in glob(self.directory.join("rdf_nav_link.txt*").to_str().unwrap()).unwrap() {
            match entry {
                Ok(path) => {
                    let file = File::open(path).unwrap();
                    let mut reader = Reader::from_reader(file);

                    for (i, line) in reader.records().enumerate() {
                        let record = line.unwrap();

                        elements.push(RdfNavLink {
                            link_id: record[0].parse().expect(&format!("could not parse {:?} as link_id in line {} of {:?}", &record[0], i, path)),
                            travel_direction: record[6].parse().expect(&format!("could not parse {:?} as travel_direction in line {} of {:?}", &record[6], i, path)),
                            speed_category: record[19].parse().expect(&format!("could not parse {:?} as speed_category in line {} of {:?}", &record[19], i, path))
                        });
                    }
                },
                Err(e) => println!("{:?}", e),
            }
        }

        elements
    }


    fn nodes(&self) -> Vec<RdfNode> {
        let elements = vec![];

        for entry in glob(self.directory.join("rdf_node.txt*").to_str().unwrap()).unwrap() {
            match entry {
                Ok(path) => {
                    let file = File::open(path).unwrap();
                    let mut reader = Reader::from_reader(file);

                    for (i, line) in reader.records().enumerate() {
                        let record = line.unwrap();

                        elements.push(RdfNode {
                            node_id: record[0].parse().expect(&format!("could not parse {:?} as node_id in line {} of {:?}", &record[0], i, path)),
                            lat: record[1].parse().expect(&format!("could not parse {:?} as lat in line {} of {:?}", &record[1], i, path)),
                            lon: record[2].parse().expect(&format!("could not parse {:?} as lon in line {} of {:?}", &record[2], i, path)),
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
        let elements = vec![];

        for entry in glob(self.directory.join("rdf_link_geometry.txt*").to_str().unwrap()).unwrap() {
            match entry {
                Ok(path) => {
                    let file = File::open(path).unwrap();
                    let mut reader = Reader::from_reader(file);

                    for (i, line) in reader.records().enumerate() {
                        let record = line.unwrap();

                        elements.push(RdfLinkGeometry {
                            link_id: record[0].parse().expect(&format!("could not parse {:?} as link_id in line {} of {:?}", &record[0], i, path)),
                            seq_num: record[1].parse().expect(&format!("could not parse {:?} as seq_num in line {} of {:?}", &record[1], i, path)),
                            lat: record[2].parse().expect(&format!("could not parse {:?} as lat in line {} of {:?}", &record[2], i, path)),
                            lon: record[3].parse().expect(&format!("could not parse {:?} as lon in line {} of {:?}", &record[3], i, path)),
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
