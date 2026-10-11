//! Spec loader mirrors tests/drivers/harness/spec_loader.py.
use serde::Deserialize;
use std::env;
use std::path::PathBuf;

pub const PROTOCOLS: [&str; 4] = [
    "simple",
    "extended-noparam",
    "extended-text",
    "extended-binary",
];

#[derive(Debug, Deserialize, Clone)]
pub struct TypeSpec {
    pub oid: i32,
    #[serde(default)]
    pub name: String,
    pub pg_typname: String,
    #[serde(default)]
    pub cast_sql: Option<String>,
    #[serde(default)]
    pub text: bool,
    #[serde(default)]
    pub binary: bool,
    #[serde(default)]
    pub samples: Vec<serde_yaml::Value>,
    #[serde(default)]
    pub skip: std::collections::HashMap<String, Vec<String>>,
}

impl TypeSpec {
    pub fn cast_sql(&self) -> String {
        self.cast_sql
            .clone()
            .unwrap_or_else(|| format!("$1::{}", self.pg_typname))
    }
    pub fn supports(&self, proto: &str) -> bool {
        if proto == "extended-binary" {
            self.binary
        } else {
            self.text
        }
    }
    pub fn is_skipped(&self, driver_key: &str, proto: &str) -> bool {
        if let Some(modes) = self.skip.get(driver_key) {
            modes.iter().any(|m| m == "all" || m == proto)
        } else {
            false
        }
    }
    pub fn name_or(&self) -> String {
        if self.name.is_empty() {
            self.pg_typname.clone()
        } else {
            self.name.clone()
        }
    }
}

pub struct Case {
    pub ty: TypeSpec,
    pub proto: String,
    pub idx: usize,
    pub sample: serde_yaml::Value,
}

fn spec_dir() -> PathBuf {
    env::var("SDB_DRV_SPEC")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap()
                .join("spec")
        })
}

pub fn load_types() -> Vec<TypeSpec> {
    let p = spec_dir().join("types.yaml");
    let s = std::fs::read_to_string(&p).expect("read types.yaml");
    serde_yaml::from_str(&s).expect("parse types.yaml")
}

pub fn cases_for(driver_key: &str) -> Vec<Case> {
    let pat = env::var("SDB_DRV_TYPES").unwrap_or_else(|_| ".*".into());
    let re = regex::Regex::new(&pat).expect("invalid SDB_DRV_TYPES regex");
    let protos: Vec<String> = env::var("SDB_DRV_PROTOCOLS")
        .unwrap_or_else(|_| PROTOCOLS.join(","))
        .split(',')
        .map(|s| s.to_string())
        .collect();
    let mut out = Vec::new();
    for ty in load_types() {
        let n = ty.name_or();
        if !re.is_match(&n) {
            continue;
        }
        if !ty.text && !ty.binary {
            continue;
        }
        for p in &protos {
            if !PROTOCOLS.contains(&p.as_str()) {
                continue;
            }
            if !ty.supports(p) {
                continue;
            }
            if ty.is_skipped(driver_key, p) {
                continue;
            }
            for (i, s) in ty.samples.iter().enumerate() {
                out.push(Case {
                    ty: ty.clone(),
                    proto: p.clone(),
                    idx: i,
                    sample: s.clone(),
                });
            }
        }
    }
    out
}

pub fn conn_string() -> String {
    let host = env::var("SDB_DRV_HOST").unwrap_or_else(|_| "localhost".into());
    let port = env::var("SDB_DRV_PORT").unwrap_or_else(|_| "5432".into());
    let db = env::var("SDB_DRV_DATABASE").unwrap_or_else(|_| "serenedb".into());
    let user = env::var("SDB_DRV_USER").unwrap_or_else(|_| "serenedb".into());
    format!("host={host} port={port} dbname={db} user={user}")
}

pub fn schema_name(driver_key: &str) -> String {
    format!(
        "drv_{}_{}",
        driver_key,
        env::var("SDB_DRV_RUN_ID").unwrap_or_else(|_| "0".into())
    )
}

/// Convert a YAML scalar sample to a string. NULL stays as None.
pub fn sample_to_str(v: &serde_yaml::Value) -> Option<String> {
    match v {
        serde_yaml::Value::Null => None,
        serde_yaml::Value::Bool(b) => Some(if *b { "t".into() } else { "f".into() }),
        serde_yaml::Value::Number(n) => Some(n.to_string()),
        serde_yaml::Value::String(s) => Some(s.clone()),
        _ => Some(serde_yaml::to_string(v).unwrap_or_default()),
    }
}
