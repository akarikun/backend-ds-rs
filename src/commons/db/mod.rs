use crate::commons::db::mongo::MongoDb;
use crate::commons::db::pg::PgDb;
use crate::commons::utils::CONFIG as config;
use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::OnceCell;

mod mongo;
mod pg;

pub(super) const TASK_RUNNING_STALE_SECS: u64 = 300;

#[async_trait]
trait DbDriver: Send + Sync {
    async fn query(&self, sql: &str, params: Vec<Value>) -> Result<Vec<Value>, String>;
    async fn transaction(&self, queries: Vec<Value>) -> Result<Vec<Value>, String>;
    async fn doc_query(&self, collection: &str, command: Value) -> Result<Vec<Value>, String>;
    async fn try_begin_task(
        &self,
        task_id: &str,
        task_type: &str,
        attempt: u64,
    ) -> Result<Option<Value>, String>;
    async fn finish_task(&self, task_id: &str, result: &Value) -> Result<(), String>;
}

static DB_CONN: OnceCell<Box<dyn DbDriver>> = OnceCell::const_new();

pub async fn init_db() -> Result<(), String> {
    let conn: Box<dyn DbDriver> = match config.db.kind.as_str() {
        "mongodb" => Box::new(MongoDb::new().await?),
        "postgresql" => Box::new(PgDb::new().await?),
        other => {
            return Err(format!("unsupported db kind: {other}"));
        }
    };

    DB_CONN
        .set(conn)
        .map_err(|_| "db already initialized".to_string())
}

pub async fn db_query(sql: &str, params: Vec<Value>) -> Result<Vec<Value>, String> {
    let conn = DB_CONN
        .get()
        .ok_or_else(|| "db not initialized".to_string())?;
    conn.query(sql, params).await
}

pub async fn db_doc_query(collection: &str, command: Value) -> Result<Vec<Value>, String> {
    let conn = DB_CONN
        .get()
        .ok_or_else(|| "db not initialized".to_string())?;
    conn.doc_query(collection, command).await
}

pub async fn db_transaction(queries: Vec<Value>) -> Result<Vec<Value>, String> {
    let conn = DB_CONN
        .get()
        .ok_or_else(|| "db not initialized".to_string())?;
    conn.transaction(queries).await
}

pub async fn try_begin_task(
    task_id: &str,
    task_type: &str,
    attempt: u64,
) -> Result<Option<Value>, String> {
    let conn = DB_CONN
        .get()
        .ok_or_else(|| "db not initialized".to_string())?;
    conn.try_begin_task(task_id, task_type, attempt).await
}

pub async fn finish_task(task_id: &str, result: &Value) -> Result<(), String> {
    let conn = DB_CONN
        .get()
        .ok_or_else(|| "db not initialized".to_string())?;
    conn.finish_task(task_id, result).await
}
