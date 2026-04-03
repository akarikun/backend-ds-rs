use crate::commons::db::mongo::MongoDb;
use crate::commons::db::pg::PgDb;
use crate::commons::utils::CONFIG as config;
use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::OnceCell;

mod mongo;
mod pg;

#[async_trait]
trait DbDriver: Send + Sync {
    async fn query(&self, sql: &str, params: Vec<Value>) -> Result<Vec<Value>, String>;
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
