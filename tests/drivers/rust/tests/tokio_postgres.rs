use serenedb_drivers_rust as spec;
use tokio_postgres::{Client, NoTls};

const DRIVER_KEY: &str = "rust_tokio_postgres";

async fn connect() -> Option<Client> {
    let conn_str = spec::conn_string();
    match tokio_postgres::connect(&conn_str, NoTls).await {
        Ok((client, conn)) => {
            tokio::spawn(async move {
                if let Err(e) = conn.await {
                    eprintln!("connection error: {e}");
                }
            });
            Some(client)
        }
        Err(e) => {
            eprintln!("could not connect: {e}");
            None
        }
    }
}

async fn ensure_schema(client: &Client) -> String {
    let schema = spec::schema_name(DRIVER_KEY);
    client
        .batch_execute(&format!(
            r#"CREATE SCHEMA IF NOT EXISTS "{schema}";
               SET search_path TO "{schema}", public, pg_catalog;"#
        ))
        .await
        .unwrap();
    schema
}

#[tokio::test]
async fn smoke_select_one() {
    let Some(c) = connect().await else { return; };
    let _ = ensure_schema(&c).await;
    let row = c.query_one("SELECT 1", &[]).await.unwrap();
    let v: i32 = row.get(0);
    assert_eq!(v, 1);
}

#[tokio::test]
async fn smoke_server_version() {
    let Some(c) = connect().await else { return; };
    let _ = ensure_schema(&c).await;
    let row = c.query_one("SHOW server_version", &[]).await.unwrap();
    let s: String = row.get(0);
    assert!(!s.is_empty());
}

async fn server_text(
    c: &Client,
    pg_typname: &str,
    sample: Option<&str>,
) -> Option<String> {
    let s = sample?.to_string();
    let q = format!("SELECT (($1::text)::{pg_typname})::text");
    let row = c.query_one(q.as_str(), &[&s]).await.unwrap();
    row.get::<_, Option<String>>(0)
}

#[tokio::test]
async fn roundtrip_matrix() {
    let Some(c) = connect().await else { return; };
    let _ = ensure_schema(&c).await;
    let cases = spec::cases_for(DRIVER_KEY);
    if cases.is_empty() {
        eprintln!("no cases (SDB_DRV_TYPES filter?)");
        return;
    }
    let mut failures: Vec<String> = Vec::new();
    for case in cases {
        let sample = spec::sample_to_str(&case.sample);
        let expected = server_text(&c, &case.ty.pg_typname, sample.as_deref()).await;
        let pgname = &case.ty.pg_typname;
        let actual: Option<String> = match case.proto.as_str() {
            "simple" | "extended-noparam" => {
                let q = match &sample {
                    None => format!("SELECT NULL::{pgname}::text"),
                    Some(s) => {
                        let lit = s.replace('\'', "''");
                        format!("SELECT '{lit}'::{pgname}::text")
                    }
                };
                let row = c.query_one(q.as_str(), &[]).await.unwrap();
                row.get(0)
            }
            "extended-text" | "extended-binary" => {
                // tokio-postgres binds Rust types strictly to PG types. To
                // exercise the wire round-trip uniformly across types
                // without writing per-type binding code, we always bind
                // Option<String> and add a `::text` cast in front of the
                // type cast so the server treats the param as text.
                let cast = case.ty.cast_sql().replacen("$1", "$1::text", 1);
                let q = format!("SELECT ({cast})::text");
                let row = c.query_one(q.as_str(), &[&sample]).await.unwrap();
                row.get(0)
            }
            other => panic!("unknown protocol {other}"),
        };
        if actual != expected {
            failures.push(format!(
                "{}-{}-{}-{}: expected {expected:?} got {actual:?}",
                case.ty.oid, case.ty.name_or(), case.proto, case.idx
            ));
        }
    }
    if !failures.is_empty() {
        for f in &failures {
            eprintln!("FAIL: {f}");
        }
        panic!("{} round-trip failures", failures.len());
    }
}
