use rust_road_router::datastr::graph::*;
use rust_road_router::datastr::rank_select_map::{BitVec, RankSelectMap};
use rust_road_router::util::in_range_option::*;
use std::error::Error;
use std::fmt;
use std::iter;
use std::str::FromStr;

use nav_types::WGS84;

pub mod csv_source;
pub mod link_id_mapper;

#[derive(Debug)]
pub struct DirectionParseError;

impl fmt::Display for DirectionParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Here direction could not be parsed!")
    }
}

impl Error for DirectionParseError {
    fn description(&self) -> &str {
        "Here direction could not be parsed!"
    }
}

#[derive(Debug, Clone, Copy)]
pub enum RdfLinkDirection {
    FromRef,
    ToRef,
    Both,
}

impl Default for RdfLinkDirection {
    fn default() -> Self {
        RdfLinkDirection::Both
    }
}

impl FromStr for RdfLinkDirection {
    type Err = DirectionParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.chars().next() {
            Some('B') => Ok(RdfLinkDirection::Both),
            Some('F') => Ok(RdfLinkDirection::FromRef),
            Some('T') => Ok(RdfLinkDirection::ToRef),
            _ => Err(DirectionParseError),
        }
    }
}

#[derive(Debug)]
pub struct RdfLink {
    link_id: i64,
    ref_node_id: i64,
    nonref_node_id: i64,
}

#[derive(Debug, Default, Clone)]
pub struct RdfNavLink {
    link_id: i64,
    functional_class: u8,
    travel_direction: RdfLinkDirection,
    speed_category: i32,
    from_ref_speed_limit: Option<i32>,
    to_ref_speed_limit: Option<i32>,
}

impl RdfNavLink {
    fn speed_in_m_per_s(&self, direction: RdfLinkDirection) -> f64 {
        let link_speed = match self.speed_category {
            1 => 36.11,
            2 => 31.94,
            3 => 26.388,
            4 => 22.22,
            5 => 16.66,
            6 => 11.11,
            7 => 5.55,
            8 => 1.38,
            _ => panic!("unknown speed category"),
        };

        let limit = match direction {
            RdfLinkDirection::FromRef => self.from_ref_speed_limit,
            RdfLinkDirection::ToRef => self.to_ref_speed_limit,
            _ => panic!("invalid argument"),
        };

        let limit = f64::from(limit.unwrap_or(999)) / 3.6;

        if limit < link_speed {
            limit
        } else {
            link_speed
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RdfNode {
    node_id: i64,
    lat: i64,
    lon: i64,
    z_coord: Option<i64>,
}

impl RdfNode {
    fn as_wgs84(&self) -> WGS84<f64> {
        WGS84::from_degrees_and_meters(
            (self.lat as f64) / 100_000.,
            (self.lon as f64) / 100_000.,
            (self.z_coord.unwrap_or(0) as f64) / 100.,
        )
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RdfLinkGeometry {
    link_id: i64,
    seq_num: i64,
    lat: i64,
    lon: i64,
    z_coord: Option<i64>,
}

impl RdfLinkGeometry {
    fn as_wgs84(&self) -> WGS84<f64> {
        WGS84::from_degrees_and_meters(
            (self.lat as f64) / 10_000_000.,
            (self.lon as f64) / 10_000_000.,
            (self.z_coord.unwrap_or(0) as f64) / 100.,
        )
    }
}

pub struct HereData {
    pub graph: OwnedGraph,
    pub link_lengths: Vec<f64>,
    pub functional_road_classes: Vec<u8>,
    pub lat: Vec<f32>,
    pub lng: Vec<f32>,
    pub link_id_mapping: RankSelectMap,
    pub here_rank_to_link_id: Vec<(InRangeOption<EdgeId>, InRangeOption<EdgeId>)>,
}

pub trait RdfDataSource {
    fn links(&self) -> Vec<RdfLink>;
    fn nav_links(&self) -> Vec<RdfNavLink>;
    fn nodes(&self) -> Vec<RdfNode>;
    fn link_geometries(&self) -> Vec<RdfLinkGeometry>;
}

pub fn read_graph(source: &dyn RdfDataSource, (min_lat, min_lon): (i64, i64), (max_lat, max_lon): (i64, i64)) -> HereData {
    let included = |node: &RdfNode| node.lat >= min_lat && node.lat <= max_lat && node.lon >= min_lon && node.lon <= max_lon;

    eprintln!("read nodes");

    let input_nodes = source.nodes();
    let max_node_id = input_nodes.iter().map(|node| node.node_id).max().unwrap();
    let mut filtered_node_ids = BitVec::new(max_node_id as usize + 1);
    for node in input_nodes.iter().filter(|&n| included(n)) {
        filtered_node_ids.set(node.node_id as usize);
    }

    eprintln!("read nav links");
    // start with all nav links
    let mut nav_links: Vec<RdfNavLink> = source.nav_links();

    eprintln!("read links");
    let links: Vec<_> = source
        .links()
        .into_iter()
        .filter(|link| filtered_node_ids.get(link.ref_node_id as usize) && filtered_node_ids.get(link.nonref_node_id as usize))
        .collect();
    let max_here_link_id = links.iter().map(|l| l.link_id).chain(nav_links.iter().map(|l| l.link_id)).max().unwrap() as usize;
    let mut links_present = BitVec::new(max_here_link_id as usize + 1);
    for link in &links {
        links_present.set(link.link_id as usize);
    }
    let maximum_node_id = links
        .iter()
        .flat_map(|link| iter::once(link.ref_node_id).chain(iter::once(link.nonref_node_id)))
        .max()
        .unwrap();

    eprintln!("build link id mapping");
    nav_links.retain(|nav_link| links_present.get(nav_link.link_id as usize));
    // local ids for links
    let mut link_id_mapping = BitVec::new(max_here_link_id + 1);
    for nav_link in &nav_links {
        link_id_mapping.set(nav_link.link_id as usize);
    }
    let link_id_mapping = RankSelectMap::new(link_id_mapping);

    eprintln!("sort nav links");
    let mut sorted_nav_links = vec![RdfNavLink::default(); nav_links.len() + 1];
    for link in nav_links {
        let rank = link_id_mapping.get(link.link_id as usize).unwrap();
        sorted_nav_links[rank] = link;
    }
    let nav_links = sorted_nav_links;

    // a data structure to do the global to local node ids mapping
    let mut node_id_mapping = BitVec::new(maximum_node_id as usize + 1);

    eprintln!("build node id mapping");
    // insert all global node ids we encounter in links
    for link in &links {
        if link_id_mapping.get(link.link_id as usize).is_some() {
            node_id_mapping.set(link.ref_node_id as usize);
            node_id_mapping.set(link.nonref_node_id as usize);
        }
    }
    let node_id_mapping = RankSelectMap::new(node_id_mapping);

    eprintln!("build up degrees");
    // now we know the number of nodes
    let n = node_id_mapping.len();
    // vector to store degrees which will then be turned into the first_out array
    let mut degrees: Vec<u32> = vec![0; n + 1];

    // iterate over all links and count degrees for nodes
    for link in &links {
        if let Some(link_index) = link_id_mapping.get(link.link_id as usize) {
            match nav_links[link_index].travel_direction {
                RdfLinkDirection::FromRef => degrees[node_id_mapping.at(link.ref_node_id as usize)] += 1,
                RdfLinkDirection::ToRef => degrees[node_id_mapping.at(link.nonref_node_id as usize)] += 1,
                RdfLinkDirection::Both => {
                    degrees[node_id_mapping.at(link.ref_node_id as usize)] += 1;
                    degrees[node_id_mapping.at(link.nonref_node_id as usize)] += 1;
                }
            }
        }
    }

    // do prefix sum over degrees which yields first_out
    // doing in place mutation and afterwards move into new var here
    // if we'd use into_iter the compiler might figure out to do this as well, but I dont know
    let m = degrees.iter_mut().fold(0, |prefix, degree| {
        let sum = prefix + *degree;
        *degree = prefix;
        sum
    });
    let mut first_out = degrees; // move

    eprintln!("read link geometry");
    // fetch links geometry
    let mut link_geometries = vec![Vec::new(); link_id_mapping.len()];
    for geometry in source.link_geometries() {
        if let Some(link_index) = link_id_mapping.get(geometry.link_id as usize) {
            link_geometries[link_index].push(geometry);
        }
    }
    for geometries in &mut link_geometries {
        geometries.sort_by_key(|geometry| geometry.seq_num);
    }

    eprintln!("sort nodes");
    let mut nodes: Vec<RdfNode> = vec![
        RdfNode {
            node_id: 0,
            lat: 0,
            lon: 0,
            z_coord: None
        };
        n
    ];
    for node in input_nodes {
        if let Some(index) = node_id_mapping.get(node.node_id as usize) {
            nodes[index] = node;
        }
    }

    // init other graph arrays
    let mut head: Vec<NodeId> = vec![0; m as usize];
    let mut travel_times: Vec<Weight> = vec![0; m as usize];
    let mut link_lengths: Vec<f64> = vec![0.0; m as usize];
    let mut functional_road_classes: Vec<u8> = vec![0; m as usize];
    let mut here_rank_to_link_id: Vec<(InRangeOption<EdgeId>, InRangeOption<EdgeId>)> = vec![(InRangeOption::NONE, InRangeOption::NONE); links.len()];

    eprintln!("calculate weights");
    // iterate over all links and insert head and weight
    // increment the first_out values along the way
    for link in &links {
        if let Some(link_index) = link_id_mapping.get(link.link_id as usize) {
            let nav_link = &nav_links[link_index];
            let length = if link_geometries[link_index].is_empty() {
                let head = &nodes[node_id_mapping.at(link.ref_node_id as usize)];
                let tail = &nodes[node_id_mapping.at(link.nonref_node_id as usize)];
                head.as_wgs84().distance(&tail.as_wgs84())
            } else {
                calculate_length_in_m(&link_geometries[link_index])
            };
            let from_node = node_id_mapping.at(link.ref_node_id as usize);
            let to_node = node_id_mapping.at(link.nonref_node_id as usize);

            let from_weight = (1000. * length / nav_link.speed_in_m_per_s(RdfLinkDirection::FromRef)).round() as Weight;
            let to_weight = (1000. * length / nav_link.speed_in_m_per_s(RdfLinkDirection::ToRef)).round() as Weight;

            match nav_link.travel_direction {
                RdfLinkDirection::FromRef => {
                    head[first_out[from_node] as usize] = to_node as NodeId;
                    travel_times[first_out[from_node] as usize] = from_weight;
                    link_lengths[first_out[from_node] as usize] = length;
                    functional_road_classes[first_out[from_node] as usize] = nav_link.functional_class;
                    here_rank_to_link_id[link_index].0 = InRangeOption::some(first_out[from_node]);
                    first_out[from_node] += 1;
                }
                RdfLinkDirection::ToRef => {
                    head[first_out[to_node] as usize] = from_node as NodeId;
                    travel_times[first_out[to_node] as usize] = to_weight;
                    link_lengths[first_out[to_node] as usize] = length;
                    functional_road_classes[first_out[to_node] as usize] = nav_link.functional_class;
                    here_rank_to_link_id[link_index].1 = InRangeOption::some(first_out[to_node]);
                    first_out[to_node] += 1;
                }
                RdfLinkDirection::Both => {
                    head[first_out[from_node] as usize] = to_node as NodeId;
                    travel_times[first_out[from_node] as usize] = from_weight;
                    link_lengths[first_out[from_node] as usize] = length;
                    functional_road_classes[first_out[from_node] as usize] = nav_link.functional_class;
                    here_rank_to_link_id[link_index].0 = InRangeOption::some(first_out[from_node]);
                    first_out[from_node] += 1;

                    head[first_out[to_node] as usize] = from_node as NodeId;
                    travel_times[first_out[to_node] as usize] = to_weight;
                    link_lengths[first_out[to_node] as usize] = length;
                    functional_road_classes[first_out[to_node] as usize] = nav_link.functional_class;
                    here_rank_to_link_id[link_index].1 = InRangeOption::some(first_out[to_node]);
                    first_out[to_node] += 1;
                }
            }
        }
    }

    // first out values got basically incremented to the value of their successor node
    // pop the last one (wasn't changed)
    first_out.pop().unwrap();
    // insert a zero at the beginning - this will shift all values one to the right
    first_out.insert(0, 0);

    let graph = OwnedGraph::new(first_out, head, travel_times);
    let lat = nodes.iter().map(|node| ((node.lat as f64) / 100_000.) as f32).collect();
    let lng = nodes.iter().map(|node| ((node.lon as f64) / 100_000.) as f32).collect();
    HereData {
        graph,
        link_lengths,
        functional_road_classes,
        lat,
        lng,
        link_id_mapping,
        here_rank_to_link_id,
    }
}

fn calculate_length_in_m(geometries: &[RdfLinkGeometry]) -> f64 {
    geometries.windows(2).map(|pair| pair[0].as_wgs84().distance(&pair[1].as_wgs84())).sum()
}
