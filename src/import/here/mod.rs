use ::graph::first_out_graph::FirstOutGraph;
use ::rank_select_map::RankSelectMap;
use std::iter;

#[derive(Debug)]
enum RdfLinkDirection {
    FromRef,
    ToRef,
    Both
}

#[derive(Debug)]
struct RdfLink {
    link_id: i64,
    ref_node_id: i64,
    nonref_node_id: i64
}

#[derive(Debug)]
struct RdfNavLink {
    link_id: i64,
    travel_direction: RdfLinkDirection,
    speed_category: i32
}

#[derive(Debug)]
struct RdfNode {
    node_id: i64,
    lat: i64,
    lon: i64,
    z_coord: Option<i64>
}

#[derive(Debug)]
struct RdfLinkGeometry {
    link_id: i64,
    seq_num: i64,
    lat: i64,
    lon: i64,
    z_coord: Option<i64>
}

trait RdfDataSource {
    fn link_iter(&self) -> Box<Iterator<Item=RdfLink>>;
    fn nav_link_iter(&self) -> Box<Iterator<Item=RdfNavLink>>;
    fn node_iter(&self) -> Box<Iterator<Item=RdfNode>>;
    fn link_geometry_iter(&self) -> Box<Iterator<Item=RdfLinkGeometry>>;

    fn maximum_node_id(&self) -> i64 {
        // iterate over all edges and take the maximum id.
        self.link_iter().flat_map(|link| iter::once(link.ref_node_id).chain(iter::once(link.nonref_node_id)) ).max().unwrap()
    }
}

fn read_graph(source: &RdfDataSource) -> FirstOutGraph {
    let mut nav_links: Vec<RdfNavLink> = source.nav_link_iter().collect();
    nav_links.sort_by_key(|nav_link| nav_link.link_id );

    let mut link_indexes = RankSelectMap::new(nav_links.last().unwrap().link_id as usize);
    for nav_link in nav_links.iter() {
        link_indexes.insert(nav_link.link_id as usize);
    }
    link_indexes.compile();


    // a data structure to do the global to local node ids mapping
    let mut id_mapping = RankSelectMap::new(source.maximum_node_id() as usize);

    // insert all global ids we encounter in links
    for link in source.link_iter() {
        match link_indexes.get(link.link_id as usize) {
            Some(_) => {
                id_mapping.insert(link.ref_node_id as usize);
                id_mapping.insert(link.nonref_node_id as usize);
            },
            None => (),
        }
    }
    id_mapping.compile();

    // now we know the number of nodes
    let n = id_mapping.len();
    // vector to store degrees which will then be turned into the first_out array
    let mut degrees: Vec<u32> = vec![0; n + 1];

    for link in source.link_iter() {
        match link_indexes.get(link.link_id as usize) {
            Some(link_index) => {
                match nav_links[link_index].travel_direction {
                    RdfLinkDirection::FromRef => degrees[id_mapping.at(link.ref_node_id as usize)] += 1,
                    RdfLinkDirection::ToRef => degrees[id_mapping.at(link.nonref_node_id as usize)] += 1,
                    RdfLinkDirection::Both => {
                        degrees[id_mapping.at(link.ref_node_id as usize)] += 1;
                        degrees[id_mapping.at(link.nonref_node_id as usize)] += 1;
                    }
                }
            },
            None => (),
        }
    }

    let m = degrees.iter_mut().fold(0, |prefix, degree| {
        let sum = prefix + *degree;
        *degree = prefix;
        sum
    });
    let mut first_out = degrees;

    let mut head = vec![0; m as usize];
    let mut weights = vec![0; m as usize];

    for link in source.link_iter() {
        match link_indexes.get(link.link_id as usize) {
            Some(link_index) => {
                let nav_link = &nav_links[link_index];
                match nav_link.travel_direction {
                    RdfLinkDirection::FromRef => {
                        head[first_out[id_mapping.at(link.ref_node_id as usize)] as usize] = id_mapping.at(link.nonref_node_id as usize);
                        weights[first_out[id_mapping.at(link.ref_node_id as usize)] as usize] = 0; // TODO actual calculation
                        first_out[id_mapping.at(link.ref_node_id as usize)] += 1;
                    },
                    RdfLinkDirection::ToRef => {
                        head[first_out[id_mapping.at(link.nonref_node_id as usize)] as usize] = id_mapping.at(link.ref_node_id as usize);
                        weights[first_out[id_mapping.at(link.nonref_node_id as usize)] as usize] = 0; // TODO actual calculation
                        first_out[id_mapping.at(link.nonref_node_id as usize)] += 1;
                    },
                    RdfLinkDirection::Both => {
                        head[first_out[id_mapping.at(link.ref_node_id as usize)] as usize] = id_mapping.at(link.nonref_node_id as usize);
                        weights[first_out[id_mapping.at(link.ref_node_id as usize)] as usize] = 0; // TODO actual calculation
                        first_out[id_mapping.at(link.ref_node_id as usize)] += 1;

                        head[first_out[id_mapping.at(link.nonref_node_id as usize)] as usize] = id_mapping.at(link.ref_node_id as usize);
                        weights[first_out[id_mapping.at(link.nonref_node_id as usize)] as usize] = 0; // TODO actual calculation
                        first_out[id_mapping.at(link.nonref_node_id as usize)] += 1;
                    }
                }
            },
            None => (),
        }
    }

    first_out.pop().unwrap();
    first_out.insert(0, 0);

    unimplemented!();
}
