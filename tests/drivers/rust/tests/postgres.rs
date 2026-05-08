//! rust-postgres (sync) round-trip matrix.
use postgres::{Client, NoTls};
use serenedb_drivers_rust as spec;

const DRIVER_KEY: &str = "rust_postgres";

fn connect() -> Option<Client> {
    let host = std::env::var("SDB_DRV_HOST").unwrap_or_else(|_| "localhost".into());
    let port = std::env::var("SDB_DRV_PORT").unwrap_or_else(|_| "5432".into());
    let db = std::env::var("SDB_DRV_DATABASE").unwrap_or_else(|_| "postgres".into());
    let user = std::env::var("SDB_DRV_USER").unwrap_or_else(|_| "postgres".into());
    let url = format!("host={host} port={port} dbname={db} user={user}");
    match Client::connect(&url, NoTls) {
        Ok(c) => Some(c),
        Err(e) => {
            eprintln!("rust-postgres connect failed: {e}");
            None
        }
    }
}

#[test]
fn smoke_select_one() {
    let Some(mut c) = connect() else { return; };
    let row = c.query_one("SELECT 1::int4 AS v", &[]).unwrap();
    let v: i32 = row.get("v");
    assert_eq!(v, 1);
}

#[test]
fn roundtrip_matrix() {
    let Some(mut c) = connect() else { return; };
    let schema = spec::schema_name(DRIVER_KEY);
    c.batch_execute(&format!(
        r#"CREATE SCHEMA IF NOT EXISTS "{schema}";
           SET search_path TO "{schema}", public, pg_catalog;"#
    )).unwrap();

    let cases = spec::cases_for(DRIVER_KEY);
    let mut failures: Vec<String> = Vec::new();
    for case in cases {
        let sample = spec::sample_to_str(&case.sample);
        let pgname = &case.ty.pg_typname;
        let expected: Option<String> = match &sample {
            None => None,
            Some(s) => {
                let q = format!("SELECT (($1::text)::{pgname})::text AS v");
                let row = c.query_one(q.as_str(), &[s]).unwrap();
                row.get(0)
            }
        };
        let actual: Option<String> = match case.proto.as_str() {
            "simple" | "extended-noparam" => {
                let q = match &sample {
                    None => format!("SELECT NULL::{pgname}::text AS v"),
                    Some(s) => {
                        let lit = s.replace('\'', "''");
                        format!("SELECT '{lit}'::{pgname}::text AS v")
                    }
                };
                let row = c.query_one(q.as_str(), &[]).unwrap();
                row.get(0)
            }
            "extended-text" | "extended-binary" => {
                let cast = case.ty.cast_sql().replacen("$1", "$1::text", 1);
                let q = format!("SELECT ({cast})::text AS v");
                let row = c.query_one(q.as_str(), &[&sample]).unwrap();
                row.get(0)
            }
            other => panic!("unknown protocol {other}"),
        };
        if actual != expected {
            failures.push(format!(
                "{}-{}-{}-{}: expected {expected:?} got {actual:?}",
                case.ty.oid, case.ty.name_or(), case.proto, case.idx,
            ));
        }
    }
    if !failures.is_empty() {
        for f in &failures {
            eprintln!("FAIL: {f}");
        }
        panic!("{} rust-postgres round-trip failures", failures.len());
    }
}
