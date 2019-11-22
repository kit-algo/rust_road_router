#![feature(proc_macro_hygiene, decl_macro)]

#[macro_use]
extern crate rocket;
#[macro_use]
extern crate serde_derive;

use std::{
    env,
    error::Error,
    iter::once,
    path::{Path, PathBuf},
    sync::mpsc::{self, Sender},
    sync::{Arc, Mutex},
    thread,
};

use rocket::{request::Form, response::NamedFile, State};
use rocket_contrib::json::Json;

use kdtree::kdtree::{Kdtree, KdtreePointTrait};

use bmw_routing_engine::{
    algo::customizable_contraction_hierarchy::{self, query::Server},
    cli::CliErr,
    datastr::{
        graph::{link_id_to_tail_mapper::*, *},
        node_order::NodeOrder,
        rank_select_map::*,
    },
    import::here::link_id_mapper::*,
    io::*,
    report::report_time,
};

#[derive(Debug, PartialEq, Clone, Copy)]
struct NodeCoord {
    coords: [f64; 2],
    node_id: NodeId,
}

impl KdtreePointTrait for NodeCoord {
    #[inline] // the inline on this method is important! as without it there is ~25% speed loss on the tree when cross-crate usage.
    fn dims(&self) -> &[f64] {
        &self.coords
    }
}

#[derive(Debug, FromForm, Copy, Clone)]
struct GeoQuery {
    from_lat: f32,
    from_lng: f32,
    to_lat: f32,
    to_lng: f32,
}

#[derive(Debug, Serialize, Deserialize)]
struct GeoResponse {
    distance: Weight,
    path: Vec<(f32, f32)>,
}

#[derive(Debug, FromForm, Copy, Clone)]
struct HereQuery {
    from_link_id: u64,
    from_direction: bool,
    from_link_fraction: f32,
    to_link_id: u64,
    to_direction: bool,
    to_link_fraction: f32,
}

#[derive(Debug, Serialize, Deserialize)]
struct HereResponse {
    distance: Weight,
    path: Vec<(u64, bool)>,
}

#[derive(Debug)]
enum Request {
    Geo((GeoQuery, Sender<Option<GeoResponse>>)),
    Here((HereQuery, Sender<Option<HereResponse>>)),
    Customize(Vec<(u64, bool, SerializedWeight)>),
}

#[get("/")]
fn index() -> Option<NamedFile> {
    NamedFile::open(Path::new("static/index.html")).ok()
}

#[get("/<file..>")]
fn files(file: PathBuf) -> Option<NamedFile> {
    NamedFile::open(Path::new("static/").join(file)).ok()
}

#[get("/query?<query_params..>", format = "application/json")]
fn query(query_params: Form<GeoQuery>, state: State<Mutex<Sender<Request>>>) -> Json<Option<GeoResponse>> {
    let result = report_time("Total Query Request Time", || {
        println!("Received Query: {:?}", query_params);

        let tx_query = state.lock().unwrap();
        let (tx_result, rx_result) = mpsc::channel::<Option<GeoResponse>>();

        tx_query.send(Request::Geo((*query_params, tx_result))).unwrap();
        rx_result.recv().expect("routing engine crashed or hung up")
    });

    println!();
    Json(result)
}

#[get("/here_query?<query_params..>", format = "application/json")]
fn here_query(query_params: Form<HereQuery>, state: State<Mutex<Sender<Request>>>) -> Json<Option<HereResponse>> {
    let result = report_time("Total Query Request Time", || {
        println!("Received Query: {:?}", query_params);

        let tx_query = state.lock().unwrap();
        let (tx_result, rx_result) = mpsc::channel::<Option<HereResponse>>();

        tx_query.send(Request::Here((*query_params, tx_result))).unwrap();
        rx_result.recv().expect("routing engine crashed or hung up")
    });

    println!();
    Json(result)
}

#[derive(Debug)]
struct SerializedWeight(Weight);

use serde::de::{Deserialize, Deserializer};
use serde_json::Value;

impl<'de> Deserialize<'de> for SerializedWeight {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;

        let v = Value::deserialize(deserializer)?;
        match v {
            Value::Null => Ok(SerializedWeight(INFINITY)),
            Value::Number(w) => match w.as_u64() {
                Some(w) => {
                    if w < INFINITY.into() {
                        Ok(SerializedWeight(w as Weight))
                    } else {
                        Err(<D as Deserializer>::Error::custom(format!(
                            "Got {} as weight which is bigger than the max weight {}.",
                            w, INFINITY
                        )))
                    }
                }
                None => Err(<D as Deserializer>::Error::custom("Got float or negative number as weight")),
            },
            _ => Err(<D as Deserializer>::Error::custom("Got invalid JSON Value for Weight, expected null or number")),
        }
    }
}

#[post("/customize", data = "<updates>")]
fn customize(updates: Json<Vec<(u64, bool, SerializedWeight)>>, state: State<Mutex<Sender<Request>>>) {
    let tx_query = state.lock().unwrap();
    tx_query.send(Request::Customize(updates.0)).expect("routing engine crashed or hung up");
}

fn main() -> Result<(), Box<dyn Error>> {
    let (tx_query, rx_query) = mpsc::channel::<Request>();

    let mut args = env::args();
    args.next();

    let arg = &args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let first_out = Vec::load_from(path.join("first_out"))?;
    let head = Vec::load_from(path.join("head"))?;
    let travel_time = Vec::load_from(path.join("travel_time"))?;

    let lat = Vec::load_from(path.join("latitude"))?;
    let lng = Vec::load_from(path.join("longitude"))?;

    let mut coords: Vec<NodeCoord> = lat
        .iter()
        .zip(lng.iter())
        .enumerate()
        .map(|(node_id, (&lat, &lng))| NodeCoord {
            node_id: node_id as NodeId,
            coords: [f64::from(lat), f64::from(lng)],
        })
        .collect();
    let tree = report_time("build kd tree", || Kdtree::new(&mut coords));

    let link_id_mapping = BitVec::load_from(path.join("link_id_mapping"))?;
    let link_id_mapping = InvertableRankSelectMap::new(RankSelectMap::new(link_id_mapping));
    let here_rank_to_link_id = Vec::load_from(path.join("here_rank_to_link_id"))?;
    let cch_order = Vec::load_from(path.join("cch_perm"))?;

    thread::spawn(move || {
        let id_mapper = LinkIdMapper::new(link_id_mapping, here_rank_to_link_id, head.len());

        let graph = FirstOutGraph::new(&first_out[..], &head[..], travel_time.clone());

        let link_id_to_tail_mapper = LinkIdToTailMapper::new(&graph);

        let cch = customizable_contraction_hierarchy::contract(&graph, NodeOrder::from_node_order(cch_order));

        let server = Arc::new(Mutex::new(Server::new(&cch, &graph)));

        let coords = |node: NodeId| -> (f32, f32) { (lat[node as usize], lng[node as usize]) };

        let closest_node = |(p_lat, p_lng): (f32, f32)| -> NodeId {
            tree.nearest_search(&NodeCoord {
                coords: [f64::from(p_lat), f64::from(p_lng)],
                node_id: 0,
            })
            .node_id
        };

        crossbeam_utils::thread::scope(|scope| {
            for query_params in rx_query {
                match query_params {
                    Request::Geo((
                        GeoQuery {
                            from_lat,
                            from_lng,
                            to_lat,
                            to_lng,
                        },
                        tx_result,
                    )) => {
                        let (from, to) = report_time("match nodes", || (closest_node((from_lat, from_lng)), closest_node((to_lat, to_lng))));

                        let mut server = server.lock().unwrap();
                        let result = report_time("cch query", || {
                            server.distance(from, to).map(|distance| {
                                let path = server.path().iter().map(|&node| coords(node)).collect();
                                GeoResponse { distance, path }
                            })
                        });

                        tx_result.send(result).unwrap();
                    }
                    Request::Here((
                        HereQuery {
                            from_link_id,
                            from_direction,
                            from_link_fraction,
                            to_link_id,
                            to_direction,
                            to_link_fraction,
                        },
                        tx_result,
                    )) => {
                        let from_link_direction = if from_direction { LinkDirection::FromRef } else { LinkDirection::ToRef };
                        let from_link_local_id = id_mapper.here_to_local_link_id(from_link_id, from_link_direction).expect("non existing link");
                        let from_link = graph.link(from_link_local_id);
                        let from = from_link.node;

                        let to_link_direction = if to_direction { LinkDirection::FromRef } else { LinkDirection::ToRef };
                        let to_link_local_id = id_mapper.here_to_local_link_id(to_link_id, to_link_direction).expect("non existing link");
                        let to_link = graph.link(to_link_local_id);
                        let to = link_id_to_tail_mapper.link_id_to_tail(to_link_local_id);

                        let mut server = server.lock().unwrap();
                        let result = report_time("cch query", || {
                            server.distance(from, to).map(|distance| {
                                let path = server.path();
                                let path_iter = path.iter();
                                let mut second_node_iter = path_iter.clone();
                                second_node_iter.next();

                                let path = once((from_link_id, from_direction))
                                    .chain(
                                        path_iter
                                            .zip(second_node_iter)
                                            .map(|(first_node, second_node)| graph.edge_index(*first_node, *second_node).unwrap())
                                            .map(|link_id| {
                                                let (id, dir) = id_mapper.local_to_here_link_id(link_id);
                                                (id, dir == LinkDirection::FromRef)
                                            }),
                                    )
                                    .chain(once((to_link_id, to_direction)))
                                    .collect();

                                let distance =
                                    distance + (from_link_fraction * from_link.weight as f32) as u32 + (to_link_fraction * to_link.weight as f32) as u32;
                                HereResponse { distance, path }
                            })
                        });

                        tx_result.send(result).unwrap();
                    }
                    Request::Customize(updates) => {
                        let server = server.clone();
                        let mut travel_time = travel_time.clone();
                        let id_mapper = &id_mapper;
                        let cch = &cch;
                        let first_out = &first_out;
                        let head = &head;

                        scope.spawn(move || {
                            for (here_link_id, is_from_ref, weight) in updates.into_iter() {
                                if is_from_ref {
                                    if let Some(link_idx) = id_mapper.here_to_local_link_id(here_link_id, LinkDirection::FromRef) {
                                        travel_time[link_idx as usize] = weight.0
                                    }
                                } else if let Some(link_idx) = id_mapper.here_to_local_link_id(here_link_id, LinkDirection::ToRef) {
                                    travel_time[link_idx as usize] = weight.0
                                }
                            }
                            *server.lock().unwrap() = Server::new(&cch, &FirstOutGraph::new(&first_out[..], &head[..], travel_time));
                        });
                    }
                }
            }
        });
    });

    rocket::ignite()
        .mount("/", routes![index, files, query, here_query, customize])
        .manage(Mutex::new(tx_query))
        .launch();

    Ok(())
}
