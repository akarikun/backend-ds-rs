use crate::commons::db::{DbDriver, TASK_RUNNING_STALE_SECS};
use crate::commons::utils::CONFIG as config;
use async_trait::async_trait;
use serde_json::{Value, json};
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::{Column, PgPool, Row, TypeInfo};
use std::time::{Duration, SystemTime};

pub struct PgDb {
    pool: PgPool,
}

impl PgDb {
    pub async fn new() -> Result<Self, String> {
        let pool = PgPoolOptions::new()
            .max_connections(16)
            .connect(&config.db.sql_url)
            .await
            .map_err(|err| err.to_string())?;

        Ok(Self { pool })
    }
}

#[async_trait]
impl DbDriver for PgDb {
    async fn query(&self, sql: &str, params: Vec<Value>) -> Result<Vec<Value>, String> {
        let mut conn = self.pool.acquire().await.map_err(|err| err.to_string())?;
        pg_query_rows(&mut *conn, sql, params).await
    }

    async fn transaction(&self, queries: Vec<Value>) -> Result<Vec<Value>, String> {
        let mut tx = self.pool.begin().await.map_err(|err| err.to_string())?;
        let mut results = Vec::with_capacity(queries.len());

        for query in queries.iter() {
            let sql = query
                .get("query")
                .or_else(|| query.get("sql"))
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .trim();
            if sql.is_empty() {
                return Err("transaction query sql is empty".to_string());
            }

            let params = query
                .get("params")
                .and_then(|value| value.as_array())
                .cloned()
                .unwrap_or_default();
            let rows = pg_query_rows(&mut *tx, sql, params).await?;
            results.push(Value::Array(rows));
        }

        tx.commit().await.map_err(|err| err.to_string())?;
        Ok(results)
    }

    async fn doc_query(&self, _collection: &str, _command: Value) -> Result<Vec<Value>, String> {
        Err("db.doc_query only supports mongodb, use db.db_query for sql databases".to_string())
    }

    async fn try_begin_task(
        &self,
        task_id: &str,
        task_type: &str,
        attempt: u64,
    ) -> Result<Option<Value>, String> {
        let attempt = attempt as i64;
        let inserted = sqlx::query(
            "insert into task_idempotency (task_id, task_type, status, attempt)
                 values ($1, $2, 'running', $3)
                 on conflict (task_id) do nothing",
        )
        .bind(task_id)
        .bind(task_type)
        .bind(attempt)
        .execute(&self.pool)
        .await
        .map_err(|err| err.to_string())?
        .rows_affected();
        if inserted > 0 {
            return Ok(None);
        }

        let stale_before = SystemTime::now()
            .checked_sub(Duration::from_secs(TASK_RUNNING_STALE_SECS))
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let stale_before = chrono::DateTime::<chrono::Utc>::from(stale_before);
        let reclaimed = sqlx::query(
            "update task_idempotency
                 set task_type = $2,
                     status = 'running',
                     attempt = $3,
                     result = null,
                     updated_at = now()
                 where task_id = $1
                   and status = 'running'
                   and attempt <= $3
                   and updated_at < $4",
        )
        .bind(task_id)
        .bind(task_type)
        .bind(attempt)
        .bind(stale_before)
        .execute(&self.pool)
        .await
        .map_err(|err| err.to_string())?
        .rows_affected();
        if reclaimed > 0 {
            return Ok(None);
        }

        let row = sqlx::query("select status, result from task_idempotency where task_id = $1")
            .bind(task_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|err| err.to_string())?;
        let status: String = row.try_get(0).map_err(|err| err.to_string())?;
        if status == "done" {
            let result = row.try_get::<Value, usize>(1).ok().unwrap_or(Value::Null);
            return Ok(Some(result));
        }

        Err("task_running".to_string())
    }

    async fn finish_task(&self, task_id: &str, result: &Value) -> Result<(), String> {
        sqlx::query(
            "update task_idempotency
                 set status = 'done', result = $2, updated_at = now()
                 where task_id = $1",
        )
        .bind(task_id)
        .bind(result)
        .execute(&self.pool)
        .await
        .map_err(|err| err.to_string())?;
        Ok(())
    }
}

async fn pg_query_rows<'e, E>(
    executor: E,
    sql: &str,
    params: Vec<Value>,
) -> Result<Vec<Value>, String>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let sql = normalize_sql_placeholders(sql);
    let mut query = sqlx::query(sql.as_str());
    for param in params.into_iter() {
        query = pg_bind_param(query, param);
    }

    let rows = query
        .fetch_all(executor)
        .await
        .map_err(|err| err.to_string())?;
    rows.into_iter()
        .map(pg_row_to_json)
        .collect::<Result<Vec<_>, _>>()
}

fn normalize_sql_placeholders(sql: &str) -> String {
    let mut index = 1;
    let mut out = String::with_capacity(sql.len());
    for ch in sql.chars() {
        if ch == '?' {
            out.push('$');
            out.push_str(index.to_string().as_str());
            index += 1;
        } else {
            out.push(ch);
        }
    }
    out
}

fn pg_bind_param<'q>(
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    value: Value,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    match value {
        Value::Bool(v) => query.bind(v),
        Value::Number(v) => {
            if let Some(v) = v.as_i64() {
                query.bind(v)
            } else {
                let v = v.as_f64().unwrap_or_default();
                if v.fract() == 0.0 {
                    query.bind(v as i64)
                } else {
                    query.bind(v)
                }
            }
        }
        Value::String(v) => query.bind(v),
        Value::Array(v) => query.bind(Value::Array(v)),
        Value::Object(v) => query.bind(Value::Object(v)),
        Value::Null => query.bind(Option::<String>::None),
    }
}

fn pg_row_to_json(row: PgRow) -> Result<Value, String> {
    let mut item = serde_json::Map::new();
    for (index, column) in row.columns().iter().enumerate() {
        let type_name = column.type_info().name().to_ascii_uppercase();
        let value = match type_name.as_str() {
            "BOOL" => json!(
                row.try_get::<Option<bool>, _>(index)
                    .map_err(|err| err.to_string())?
            ),
            "INT2" => json!(
                row.try_get::<Option<i16>, _>(index)
                    .map_err(|err| err.to_string())?
            ),
            "INT4" => json!(
                row.try_get::<Option<i32>, _>(index)
                    .map_err(|err| err.to_string())?
            ),
            "INT8" => json!(
                row.try_get::<Option<i64>, _>(index)
                    .map_err(|err| err.to_string())?
            ),
            "FLOAT4" => {
                json!(
                    row.try_get::<Option<f32>, _>(index)
                        .map_err(|err| err.to_string())?
                )
            }
            "FLOAT8" => {
                json!(
                    row.try_get::<Option<f64>, _>(index)
                        .map_err(|err| err.to_string())?
                )
            }
            "VARCHAR" | "TEXT" | "BPCHAR" | "NAME" => {
                json!(
                    row.try_get::<Option<String>, _>(index)
                        .map_err(|err| err.to_string())?
                )
            }
            "JSON" | "JSONB" => row
                .try_get::<Option<Value>, _>(index)
                .map_err(|err| err.to_string())?
                .unwrap_or(Value::Null),
            "TIMESTAMPTZ" => {
                let value = row
                    .try_get::<Option<chrono::DateTime<chrono::Utc>>, _>(index)
                    .map_err(|err| err.to_string())?
                    .map(|ts| ts.timestamp_millis())
                    .unwrap_or(0);
                json!(value)
            }
            _ => json!(type_name),
        };
        item.insert(column.name().to_string(), value);
    }
    Ok(Value::Object(item))
}
