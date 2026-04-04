use crate::commons::db::{DbDriver, TASK_RUNNING_STALE_SECS};
use crate::commons::utils::CONFIG as config;
use async_trait::async_trait;
use serde_json::{Value, json};
use std::time::{Duration, SystemTime};
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Client, NoTls, Row};

pub struct PgDb {
    client: Client,
}

impl PgDb {
    pub async fn new() -> Result<Self, String> {
        let (client, connection) = tokio_postgres::connect(&config.db.postgresql_url, NoTls)
            .await
            .map_err(|err| err.to_string())?;

        tokio::spawn(async move {
            if let Err(err) = connection.await {
                dbg!("postgres_connection_error", err.to_string());
            }
        });

        Ok(Self { client })
    }
}

#[async_trait]
impl DbDriver for PgDb {
    async fn query(&self, sql: &str, params: Vec<Value>) -> Result<Vec<Value>, String> {
        let sql = normalize_sql_placeholders(sql);
        let values = params.into_iter().map(pg_param_value).collect::<Vec<_>>();
        let refs = values
            .iter()
            .map(|value| value.as_ref() as &(dyn ToSql + Sync))
            .collect::<Vec<_>>();

        let rows = self
            .client
            .query(sql.as_str(), refs.as_slice())
            .await
            .map_err(|err| err.to_string())?;
        Ok(rows.iter().map(pg_row_to_json).collect())
    }

    async fn try_begin_task(
        &self,
        task_id: &str,
        task_type: &str,
        attempt: u64,
    ) -> Result<Option<Value>, String> {
        let attempt = attempt as i64;
        let inserted = self
            .client
            .execute(
                "insert into task_idempotency (task_id, task_type, status, attempt)
                 values ($1, $2, 'running', $3)
                 on conflict (task_id) do nothing",
                &[&task_id, &task_type, &attempt],
            )
            .await
            .map_err(|err| err.to_string())?;
        if inserted > 0 {
            return Ok(None);
        }

        let stale_before = SystemTime::now()
            .checked_sub(Duration::from_secs(TASK_RUNNING_STALE_SECS))
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let reclaimed = self
            .client
            .execute(
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
                &[&task_id, &task_type, &attempt, &stale_before],
            )
            .await
            .map_err(|err| err.to_string())?;
        if reclaimed > 0 {
            return Ok(None);
        }

        let row = self
            .client
            .query_one(
                "select status, result from task_idempotency where task_id = $1",
                &[&task_id],
            )
            .await
            .map_err(|err| err.to_string())?;
        let status: String = row.get(0);
        if status == "done" {
            let result = row.try_get::<usize, Value>(1).ok().unwrap_or(Value::Null);
            return Ok(Some(result));
        }

        Err("task_running".to_string())
    }

    async fn finish_task(&self, task_id: &str, result: &Value) -> Result<(), String> {
        self.client
            .execute(
                "update task_idempotency
                 set status = 'done', result = $2, updated_at = now()
                 where task_id = $1",
                &[&task_id, &result],
            )
            .await
            .map_err(|err| err.to_string())?;
        Ok(())
    }
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

fn pg_param_value(value: Value) -> Box<dyn ToSql + Sync + Send> {
    match value {
        Value::Bool(v) => Box::new(v),
        Value::Number(v) => {
            if let Some(v) = v.as_i64() {
                Box::new(v)
            } else {
                let v = v.as_f64().unwrap_or_default();
                if v.fract() == 0.0 {
                    Box::new(v as i64)
                } else {
                    Box::new(v)
                }
            }
        }
        Value::String(v) => Box::new(v),
        Value::Array(v) => Box::new(Value::Array(v)),
        Value::Object(v) => Box::new(Value::Object(v)),
        Value::Null => Box::new(Option::<String>::None),
    }
}

fn pg_row_to_json(row: &Row) -> Value {
    let mut item = serde_json::Map::new();
    for (index, column) in row.columns().iter().enumerate() {
        let value = match *column.type_() {
            Type::BOOL => json!(row.get::<usize, bool>(index)),
            Type::INT2 => json!(row.get::<usize, i16>(index)),
            Type::INT4 => json!(row.get::<usize, i32>(index)),
            Type::INT8 => json!(row.get::<usize, i64>(index)),
            Type::FLOAT4 => json!(row.get::<usize, f32>(index)),
            Type::FLOAT8 => json!(row.get::<usize, f64>(index)),
            Type::VARCHAR | Type::TEXT | Type::BPCHAR | Type::NAME => {
                json!(row.get::<usize, String>(index))
            }
            Type::JSON | Type::JSONB => row.get::<usize, Value>(index),
            Type::TIMESTAMPTZ => json!(
                row.get::<usize, SystemTime>(index)
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map(|ts| ts.as_millis() as i64)
                    .unwrap_or(0)
            ),
            _ => json!(format!("{:?}", column.type_())),
        };
        item.insert(column.name().to_string(), value);
    }
    Value::Object(item)
}
