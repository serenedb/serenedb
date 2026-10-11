use sea_schema::postgres::discovery::SchemaDiscovery;
use sea_schema::sea_query::PostgresQueryBuilder;
use serenedb_drivers_rust::target::{self, Database};
use sqlx::PgPool;
use std::fmt::Write;

const SCHEMA: &str = "
CREATE TABLE teams (id serial PRIMARY KEY, name varchar(40) NOT NULL UNIQUE, founded date, budget numeric(12,2) DEFAULT 0);
CREATE TABLE players (id bigserial PRIMARY KEY, team_id integer NOT NULL REFERENCES teams (id), nick text NOT NULL, mmr integer CHECK (mmr >= 0), active boolean DEFAULT true, joined timestamptz DEFAULT now(), UNIQUE (team_id, nick));
CREATE INDEX players_mmr_idx ON players (mmr);
CREATE TYPE lane AS ENUM ('safe', 'mid', 'off');
CREATE TABLE roles (id smallint PRIMARY KEY, lane lane NOT NULL, tags text[]);
";

#[tokio::test]
async fn discovery_matches_postgresql() {
    let db = Database::fresh("sea_schema").await;
    let pool = PgPool::connect(&db.url).await.unwrap();
    sqlx::raw_sql(SCHEMA).execute(&pool).await.unwrap();
    let mut schema = SchemaDiscovery::new(pool.clone(), "public").discover().await.unwrap();
    schema.tables.sort_by(|a, b| a.info.name.cmp(&b.info.name));
    let mut out = String::new();
    for statement in schema.write() {
        writeln!(out, "{};", statement.to_string(PostgresQueryBuilder)).unwrap();
    }
    writeln!(out, "{schema:#?}").unwrap();
    target::compare_golden(&target::fixture("sea_schema/discovery.txt"), &out).await.unwrap();
    pool.close().await;
    db.drop().await;
}
