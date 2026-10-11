use serenedb_drivers_rust::target::{self, Database};
use sqlx::migrate::Migrator;
use sqlx::postgres::PgListener;
use sqlx::{AssertSqlSafe, Column, Connection, Either, Executor, PgConnection, SqlSafeStr, Statement, TypeInfo};
use std::fmt::Write;
use std::time::Duration;

async fn migrator(locking: bool) -> Migrator {
    let dir = std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations"));
    let mut migrator = Migrator::new(dir).await.expect("load migrations");
    migrator.set_locking(locking);
    migrator
}

async fn applied(conn: &mut PgConnection) -> Vec<(i64, bool)> {
    sqlx::query_as("SELECT version, success FROM _sqlx_migrations ORDER BY version")
        .fetch_all(conn)
        .await
        .expect("applied migrations")
}

#[tokio::test]
async fn migrator_runs_and_undoes_reversible_migrations() {
    let db = Database::fresh("sqlx_migrate").await;
    let mut conn = PgConnection::connect(&db.url).await.unwrap();
    let migrator = migrator(false).await;
    migrator.run(&mut conn).await.unwrap();
    assert_eq!(applied(&mut conn).await, vec![(20260101000000, true), (20260102000000, true)]);
    let teams: i64 = sqlx::query_scalar("SELECT count(*) FROM teams").fetch_one(&mut conn).await.unwrap();
    assert_eq!(teams, 1);
    migrator.run(&mut conn).await.unwrap();
    migrator.undo(&mut conn, 20260101000000).await.unwrap();
    assert_eq!(applied(&mut conn).await, vec![(20260101000000, true)]);
    migrator.undo(&mut conn, 0).await.unwrap();
    assert!(applied(&mut conn).await.is_empty());
    conn.close().await.unwrap();
    db.drop().await;
}

#[tokio::test]
async fn migrator_locks_with_advisory_lock() {
    let db = Database::fresh("sqlx_lock").await;
    target::xfail_on_serenedb(
        "SereneDB has no pg_advisory_lock(), SQLx's migrator locks with it by default",
        || async {
            let mut conn = PgConnection::connect(&db.url).await?;
            migrator(true).await.run(&mut conn).await?;
            conn.close().await?;
            Ok(())
        },
    )
    .await;
    db.drop().await;
}

const DESCRIBED: &[&str] = &[
    "SELECT id, name, founded, budget FROM teams WHERE id = $1",
    "SELECT p.nick, t.name, p.mmr, p.joined FROM players p JOIN teams t ON t.id = p.team_id WHERE p.mmr > $1",
    "SELECT t.name, p.nick FROM teams t LEFT JOIN players p ON p.team_id = t.id",
    "SELECT count(*) AS n, max(mmr) AS best FROM players",
    "INSERT INTO teams (name, founded) VALUES ($1, $2) RETURNING id",
    "UPDATE players SET mmr = mmr + $1 WHERE nick = $2 RETURNING id, mmr",
    "SELECT $1::text AS a, $2::int8 AS b, now() AS c",
    "SELECT id FROM players WHERE nick = ANY($1)",
    "DELETE FROM players WHERE id = $1",
];

fn write_columns<'a>(out: &mut String, columns: impl Iterator<Item = (&'a sqlx::postgres::PgColumn, Option<bool>)>) {
    for (column, nullable) in columns {
        writeln!(
            out,
            "  column {} {} nullable {nullable:?} origin {:?}",
            column.name(),
            column.type_info().name(),
            column.origin()
        )
        .unwrap();
    }
}

fn write_parameters(out: &mut String, parameters: Option<&Either<Vec<sqlx::postgres::PgTypeInfo>, usize>>) {
    match parameters {
        Some(Either::Left(types)) => {
            let names: Vec<_> = types.iter().map(|t| t.name()).collect();
            writeln!(out, "  parameters {names:?}").unwrap();
        }
        Some(Either::Right(count)) => writeln!(out, "  parameters {count}").unwrap(),
        None => writeln!(out, "  parameters none").unwrap(),
    }
}

#[tokio::test]
async fn prepare_matches_postgresql() {
    let db = Database::fresh("sqlx_prepare").await;
    let mut conn = PgConnection::connect(&db.url).await.unwrap();
    migrator(false).await.run(&mut conn).await.unwrap();
    let mut out = String::new();
    for sql in DESCRIBED {
        writeln!(out, "{sql}").unwrap();
        let statement = (&mut conn).prepare(AssertSqlSafe(*sql).into_sql_str()).await.unwrap();
        write_columns(&mut out, statement.columns().iter().map(|c| (c, None)));
        write_parameters(&mut out, statement.parameters().map(|p| match p {
            Either::Left(types) => Either::Left(types.to_vec()),
            Either::Right(count) => Either::Right(count),
        }).as_ref());
    }
    target::compare_golden(&target::fixture("sqlx/prepare.txt"), &out).await.unwrap();
    conn.close().await.unwrap();
    db.drop().await;
}

#[tokio::test]
async fn describe_matches_postgresql() {
    let db = Database::fresh("sqlx_describe").await;
    let mut conn = PgConnection::connect(&db.url).await.unwrap();
    migrator(false).await.run(&mut conn).await.unwrap();
    target::xfail_on_serenedb(
        "SereneDB has no EXPLAIN (VERBOSE, FORMAT JSON), SQLx's describe infers nullability with it",
        || async {
            let mut out = String::new();
            for sql in DESCRIBED {
                writeln!(out, "{sql}")?;
                let described = (&mut conn).describe(AssertSqlSafe(*sql).into_sql_str()).await?;
                write_columns(&mut out, described.columns.iter().zip(described.nullable.iter().copied()));
                write_parameters(&mut out, described.parameters.as_ref());
            }
            target::compare_golden(&target::fixture("sqlx/describe.txt"), &out).await
        },
    )
    .await;
    conn.close().await.unwrap();
    db.drop().await;
}

#[tokio::test]
async fn transactions_queries_and_copy() {
    let db = Database::fresh("sqlx_tx").await;
    let mut conn = PgConnection::connect(&db.url).await.unwrap();
    migrator(false).await.run(&mut conn).await.unwrap();
    let mut tx = conn.begin().await.unwrap();
    sqlx::query("INSERT INTO teams (name) VALUES ($1)").bind("Liquid").execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    let mut tx = conn.begin().await.unwrap();
    sqlx::query("INSERT INTO teams (name) VALUES ($1)").bind("Ghost").execute(&mut *tx).await.unwrap();
    tx.rollback().await.unwrap();
    let names: Vec<(i32, String)> = sqlx::query_as("SELECT id, name FROM teams ORDER BY name")
        .fetch_all(&mut conn)
        .await
        .unwrap();
    assert_eq!(names.iter().map(|(_, n)| n.as_str()).collect::<Vec<_>>(), vec!["Liquid", "OG"]);
    let mut copy = conn.copy_in_raw("COPY teams (name, budget) FROM STDIN").await.unwrap();
    copy.send(b"Secret\t12.50\nNigma\t\\N\n".as_slice()).await.unwrap();
    assert_eq!(copy.finish().await.unwrap(), 2);
    let budgets: Vec<Option<String>> =
        sqlx::query_scalar("SELECT budget::text FROM teams WHERE name IN ('Secret', 'Nigma') ORDER BY name")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    assert_eq!(budgets, vec![None, Some("12.50".to_string())]);
    let total: i64 = sqlx::query_scalar("SELECT count(*) FROM teams").fetch_one(&mut conn).await.unwrap();
    assert_eq!(total, 4);
    conn.close().await.unwrap();
    db.drop().await;
}

#[tokio::test]
async fn nested_transaction_uses_savepoint() {
    let db = Database::fresh("sqlx_savepoint").await;
    target::xfail_on_serenedb("SereneDB has no SAVEPOINT (#722), SQLx nests transactions with savepoints", || async {
        let mut conn = PgConnection::connect(&db.url).await?;
        sqlx::raw_sql("CREATE TABLE marks (v int)").execute(&mut conn).await?;
        let mut outer = conn.begin().await?;
        sqlx::query("INSERT INTO marks VALUES (1)").execute(&mut *outer).await?;
        let mut inner = outer.begin().await?;
        sqlx::query("INSERT INTO marks VALUES (2)").execute(&mut *inner).await?;
        inner.rollback().await?;
        outer.commit().await?;
        let values: Vec<i32> = sqlx::query_scalar("SELECT v FROM marks").fetch_all(&mut conn).await?;
        if values != vec![1] {
            return Err(format!("marks {values:?}").into());
        }
        conn.close().await?;
        Ok(())
    })
    .await;
    db.drop().await;
}

#[tokio::test]
async fn listen_notify() {
    let db = Database::fresh("sqlx_listen").await;
    target::xfail_on_serenedb("SereneDB has no LISTEN/NOTIFY (#723)", || async {
        let mut listener = PgListener::connect(&db.url).await?;
        listener.listen("drv_channel").await?;
        let mut conn = PgConnection::connect(&db.url).await?;
        sqlx::query("SELECT pg_notify('drv_channel', 'hook')").execute(&mut conn).await?;
        let notification = tokio::time::timeout(Duration::from_secs(5), listener.recv()).await??;
        if notification.payload() != "hook" {
            return Err(format!("payload {}", notification.payload()).into());
        }
        Ok(())
    })
    .await;
    db.drop().await;
}
