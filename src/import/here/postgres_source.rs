use postgres::{Connection, TlsMode, Result};
use std::cmp::max;
use super::*;

#[derive(Debug)]
pub struct PostgresSource {
    connection: Connection
}

impl PostgresSource {
    pub fn new(db_url: &str) -> Result<PostgresSource> {
        let connection = Connection::connect(db_url, TlsMode::None)?;
        Ok(PostgresSource { connection })
    }
}

impl RdfDataSource for PostgresSource {
    fn links(&self) -> Vec<RdfLink> {
        self.connection
            .query("select link_id, ref_node_id, nonref_node_id from rdf_link", &[])
            .unwrap()
            .into_iter()
            .map(|row| { RdfLink { link_id: row.get(0), ref_node_id: row.get(1), nonref_node_id: row.get(2) } })
            .collect()
    }

    fn nav_links(&self) -> Vec<RdfNavLink> {
        self.connection
            .query("select link_id, travel_direction, speed_category from rdf_nav_link", &[])
            .unwrap()
            .into_iter()
            .map(|row| { RdfNavLink { link_id: row.get(0), travel_direction: sql_to_link_direction(row.get(1)), speed_category: row.get(2) } })
            .collect()
    }


    fn nodes(&self) -> Vec<RdfNode> {
        self.connection
            .query("select node_id, lat, lon, z_coord from rdf_node", &[])
            .unwrap()
            .into_iter()
            .map(|row| { RdfNode { node_id: row.get(0), lat: row.get(1), lon: row.get(2), z_coord: row.get(3) } })
            .collect()
    }

    fn link_geometries(&self) -> Vec<RdfLinkGeometry> {
        self.connection
            .query("select link_id, seq_num, lat, lon, z_coord from rdf_link_geometry", &[])
            .unwrap()
            .into_iter()
            .map(|row| { RdfLinkGeometry { link_id: row.get(0), seq_num: row.get(1), lat: row.get(2), lon: row.get(3), z_coord: row.get(4) } })
            .collect()
    }

    fn maximum_node_id(&self) -> i64 {
        let results = self.connection.query("select max(ref_node_id), max(nonref_node_id) from rdf_link", &[]).unwrap();
        let row = results.get(0);
        max(row.get(0), row.get(1))
    }
}

fn sql_to_link_direction(chararacters: String) -> RdfLinkDirection {
    match chararacters.chars().next().unwrap() {
        'B' => RdfLinkDirection::Both, // 'B'
        'F' => RdfLinkDirection::FromRef, // 'F'
        'T' => RdfLinkDirection::ToRef, // 'T'
        _ => panic!("invalid link direction")
    }
}
