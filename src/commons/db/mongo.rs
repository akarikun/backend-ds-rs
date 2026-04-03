use crate::commons::db::DbDriver;
use crate::commons::utils::CONFIG as config;
use async_trait::async_trait;
use futures::TryStreamExt;
use mongodb::bson::{Bson, Document, doc, from_bson};
use mongodb::{Client, Database};
use serde_json::{Value, json};

pub struct MongoDb {
    db: Database,
}

impl MongoDb {
    pub async fn new() -> Result<Self, String> {
        let client = Client::with_uri_str(&config.db.mongodb_uri)
            .await
            .map_err(|err| err.to_string())?;
        Ok(Self {
            db: client.database(&config.db.mongodb_db),
        })
    }
}

#[async_trait]
impl DbDriver for MongoDb {
    async fn read(&self, target: &str, query: Value) -> Result<Vec<Value>, String> {
        let filter = if query.is_null() {
            doc! {}
        } else {
            from_bson::<Document>(json_to_bson(query)).map_err(|err| err.to_string())?
        };

        let mut cursor = self
            .db
            .collection::<Document>(target)
            .find(filter)
            .await
            .map_err(|err| err.to_string())?;

        let mut rows = Vec::new();
        while let Some(doc) = cursor.try_next().await.map_err(|err| err.to_string())? {
            rows.push(bson_to_json(Bson::Document(doc)));
        }
        Ok(rows)
    }

    async fn create_player_info(&self, userid: &str, nickname: &str) -> Result<Value, String> {
        let doc = doc! {
            "userid": userid,
            "nickname": nickname,
            "created_at": chrono::Utc::now().timestamp_millis(),
        };
        let result = self
            .db
            .collection::<Document>("players")
            .insert_one(doc)
            .await
            .map_err(|err| err.to_string())?;

        Ok(json!({
            "userid": userid,
            "nickname": nickname,
            "inserted_id": bson_to_json(result.inserted_id),
        }))
    }
}

fn json_to_bson(value: Value) -> Bson {
    match value {
        Value::Null => Bson::Null,
        Value::Bool(v) => Bson::Boolean(v),
        Value::Number(v) => {
            if let Some(v) = v.as_i64() {
                Bson::Int64(v)
            } else if let Some(v) = v.as_u64() {
                Bson::Int64(v as i64)
            } else if let Some(v) = v.as_f64() {
                Bson::Double(v)
            } else {
                Bson::Null
            }
        }
        Value::String(v) => Bson::String(v),
        Value::Array(values) => Bson::Array(values.into_iter().map(json_to_bson).collect()),
        Value::Object(values) => Bson::Document(
            values
                .into_iter()
                .map(|(key, value)| (key, json_to_bson(value)))
                .collect(),
        ),
    }
}

fn bson_to_json(value: Bson) -> Value {
    match value {
        Bson::Double(v) => json!(v),
        Bson::String(v) => json!(v),
        Bson::Array(v) => Value::Array(v.into_iter().map(bson_to_json).collect()),
        Bson::Document(v) => Value::Object(
            v.into_iter()
                .map(|(key, value)| (key, bson_to_json(value)))
                .collect(),
        ),
        Bson::Boolean(v) => json!(v),
        Bson::Null => Value::Null,
        Bson::Int32(v) => json!(v),
        Bson::Int64(v) => json!(v),
        Bson::ObjectId(v) => json!(v.to_hex()),
        Bson::DateTime(v) => json!(v.timestamp_millis()),
        other => json!(other.to_string()),
    }
}
