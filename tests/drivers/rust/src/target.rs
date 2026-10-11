use sqlx::{Connection, PgConnection};
use std::env;
use std::future::Future;
use std::path::{Path, PathBuf};
use tokio::sync::OnceCell;

pub type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

pub fn url(database: &str) -> String {
    let host = env::var("SDB_DRV_HOST").unwrap_or_else(|_| "localhost".into());
    let port = env::var("SDB_DRV_PORT").unwrap_or_else(|_| "5432".into());
    let user = env::var("SDB_DRV_USER").unwrap_or_else(|_| "postgres".into());
    format!("postgres://{user}@{host}:{port}/{database}")
}

pub fn admin_url() -> String {
    url(&env::var("SDB_DRV_DATABASE").unwrap_or_else(|_| "postgres".into()))
}

static SERENEDB: OnceCell<bool> = OnceCell::const_new();

pub async fn is_serenedb() -> bool {
    *SERENEDB
        .get_or_init(|| async {
            let mut conn = PgConnection::connect(&admin_url()).await.expect("connect");
            let version: String = sqlx::query_scalar("SELECT version()")
                .fetch_one(&mut conn)
                .await
                .expect("version");
            version.contains("SereneDB")
        })
        .await
}

pub fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures").join(name)
}

pub async fn compare_golden(path: &Path, text: &str) -> TestResult {
    let stem = path.file_stem().unwrap().to_string_lossy();
    let twin = match path.extension() {
        Some(ext) => path.with_file_name(format!("{stem}.serenedb.{}", ext.to_string_lossy())),
        None => path.with_file_name(format!("{stem}.serenedb")),
    };
    let serene = is_serenedb().await;
    if env::var("SDB_DRV_UPDATE_GOLDENS").as_deref() == Ok("1") {
        if !serene {
            std::fs::create_dir_all(path.parent().unwrap())?;
            std::fs::write(path, text)?;
        } else if std::fs::read_to_string(path)? == text {
            if twin.exists() {
                std::fs::remove_file(&twin)?;
            }
        } else {
            std::fs::write(&twin, text)?;
        }
        return Ok(());
    }
    let expected = if serene && twin.exists() { twin } else { path.to_path_buf() };
    let golden = std::fs::read_to_string(&expected)?;
    if golden != text {
        return Err(format!(
            "{} differs:\n--- expected\n{golden}\n--- actual\n{text}",
            expected.display()
        )
        .into());
    }
    Ok(())
}

pub async fn xfail_on_serenedb<F, Fut>(reason: &str, body: F)
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = TestResult>,
{
    let result = body().await;
    if !is_serenedb().await {
        if let Err(e) = result {
            panic!("{e}");
        }
        return;
    }
    match result {
        Ok(()) => panic!("SereneDB passes now, drop the expected failure: {reason}"),
        Err(e) => {
            if env::var("SDB_DRV_DEBUG").as_deref() == Ok("true") {
                eprintln!("xfail on SereneDB ({reason}): {e}");
            }
        }
    }
}

pub struct Database {
    pub name: String,
    pub url: String,
}

async fn admin(sql: String) {
    let mut conn = PgConnection::connect(&admin_url()).await.expect("connect");
    sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
        .execute(&mut conn)
        .await
        .expect("admin statement");
}

impl Database {
    pub async fn fresh(prefix: &str) -> Database {
        let run_id = env::var("SDB_DRV_RUN_ID").unwrap_or_else(|_| "0".into());
        let name = format!("rs_{prefix}_{run_id}");
        admin(format!(r#"DROP DATABASE IF EXISTS "{name}""#)).await;
        admin(format!(r#"CREATE DATABASE "{name}""#)).await;
        Database { url: url(&name), name }
    }

    pub async fn drop(self) {
        admin(format!(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}' AND pid <> pg_backend_pid()",
            self.name
        ))
        .await;
        admin(format!(r#"DROP DATABASE IF EXISTS "{}""#, self.name)).await;
    }
}
