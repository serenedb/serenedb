//! sqlx (Postgres driver) round-trip matrix.
use serenedb_drivers_rust as spec;
use sqlx::{Connection, PgConnection, Row};

const DRIVER_KEY: &str = "rust_sqlx";

async fn connect() -> Option<PgConnection> {
    let host = std::env::var("SDB_DRV_HOST").unwrap_or_else(|_| "localhost".into());
    let port = std::env::var("SDB_DRV_PORT").unwrap_or_else(|_| "5432".into());
    let db = std::env::var("SDB_DRV_DATABASE").unwrap_or_else(|_| "postgres".into());
    let user = std::env::var("SDB_DRV_USER").unwrap_or_else(|_| "postgres".into());
    let url = format!("postgres://{user}@{host}:{port}/{db}");
    match PgConnection::connect(&url).await {
        Ok(c) => Some(c),
        Err(e) => {
            eprintln!("sqlx connect failed: {e}");
            None
        }
    }
}

#[tokio::test]
async fn smoke_select_one() {
    let Some(mut c) = connect().await else { return; };
    let row = sqlx::query("SELECT 1::int4 AS v")
        .fetch_one(&mut c)
        .await
        .unwrap();
    let v: i32 = row.get("v");
    assert_eq!(v, 1);
}

#[tokio::test]
async fn roundtrip_matrix() {
    let Some(mut c) = connect().await else { return; };
    let schema = spec::schema_name(DRIVER_KEY);
    sqlx::query(&format!(r#"CREATE SCHEMA IF NOT EXISTS "{schema}""#))
        .execute(&mut c).await.unwrap();
    sqlx::query(&format!(r#"SET search_path TO "{schema}", public, pg_catalog"#))
        .execute(&mut c).await.unwrap();

    let cases = spec::cases_for(DRIVER_KEY);
    let mut failures: Vec<String> = Vec::new();
    for case in cases {
        let sample_str = spec::sample_to_str(&case.sample);
        let pgname = &case.ty.pg_typname;
        let expected: Option<String> = match &sample_str {
            None => None,
            Some(s) => {
                let q = format!("SELECT (($1::text)::{pgname})::text AS v");
                let row = sqlx::query(q.as_str()).bind(s).fetch_one(&mut c).await.unwrap();
                row.get(0)
            }
        };
        let actual: Option<String> = match case.proto.as_str() {
            "simple" | "extended-noparam" => {
                let q = match &sample_str {
                    None => format!("SELECT NULL::{pgname}::text AS v"),
                    Some(s) => {
                        let lit = s.replace('\'', "''");
                        format!("SELECT '{lit}'::{pgname}::text AS v")
                    }
                };
                let row = sqlx::query(q.as_str()).fetch_one(&mut c).await.unwrap();
                row.get(0)
            }
            "extended-text" | "extended-binary" => {
                let cast = case.ty.cast_sql().replacen("$1", "$1::text", 1);
                let q = format!("SELECT ({cast})::text AS v");
                let row = sqlx::query(q.as_str())
                    .bind(sample_str.as_deref())
                    .fetch_one(&mut c).await.unwrap();
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
        panic!("{} sqlx round-trip failures", failures.len());
    }
}
