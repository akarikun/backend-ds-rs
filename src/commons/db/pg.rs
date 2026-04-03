use crate::commons::db::DbDriver;
use crate::commons::utils::CONFIG as config;
use async_trait::async_trait;
use serde_json::{Value, json};
use std::time::SystemTime;
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

        client
            .batch_execute(
                "create table if not exists players (
                    userid text primary key,
                    nickname text not null,
                    created_at timestamptz not null default now()
                );",
            )
            .await
            .map_err(|err| err.to_string())?;

        Ok(Self { client })
    }
}

#[async_trait]
impl DbDriver for PgDb {
    async fn read(&self, _target: &str, query: Value) -> Result<Vec<Value>, String> {
        let sql = query
            .get("sql")
            .and_then(|value| value.as_str())
            .ok_or_else(|| "postgres query requires {\"sql\":\"...\"}".to_string())?;

        self.query(sql, Vec::new()).await
    }

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

    async fn create_player_info(&self, userid: &str, nickname: &str) -> Result<Value, String> {
        let row = self
            .client
            .query_one(
                "insert into players (userid, nickname)
                 values ($1, $2)
                 on conflict (userid)
                 do update set nickname = excluded.nickname
                 returning userid, nickname, created_at",
                &[&userid, &nickname],
            )
            .await
            .map_err(|err| err.to_string())?;

        Ok(pg_row_to_json(&row))
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
            } else if let Some(v) = v.as_f64() {
                Box::new(v)
            } else {
                Box::new(v.to_string())
            }
        }
        Value::String(v) => Box::new(v),
        Value::Null => Box::new(Option::<String>::None),
        other => Box::new(other.to_string()),
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
