use crate::commons::db::DbDriver;
use crate::commons::utils::CONFIG as config;
use async_trait::async_trait;
use serde_json::{Value, json};
use tokio_postgres::types::Type;
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

        let rows = self
            .client
            .query(sql, &[])
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
            _ => json!(format!("{:?}", column.type_())),
        };
        item.insert(column.name().to_string(), value);
    }
    Value::Object(item)
}
