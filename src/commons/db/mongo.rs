use crate::commons::db::{DbDriver, TASK_RUNNING_STALE_SECS};
use crate::commons::utils::CONFIG as config;
use async_trait::async_trait;
use futures::TryStreamExt;
use mongodb::bson::{Bson, Document, doc};
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

        let db = client.database(&config.db.mongodb_db);
        migrate_legacy_players(&db).await?;

        Ok(Self { db })
    }
}

#[async_trait]
impl DbDriver for MongoDb {
    async fn query(&self, _sql: &str, _params: Vec<Value>) -> Result<Vec<Value>, String> {
        Err("db.db_query only supports sql databases, use db.doc_query for mongodb in future addons".to_string())
    }

    async fn transaction(&self, _queries: Vec<Value>) -> Result<Vec<Value>, String> {
        Err("db.db_transaction only supports sql databases, use mongodb document transaction extension later".to_string())
    }

    async fn doc_query(&self, collection: &str, command: Value) -> Result<Vec<Value>, String> {
        let collection = collection.trim();
        if collection.is_empty() {
            return Err("collection is empty".to_string());
        }

        let command_name = command
            .get("op")
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .trim();
        let docs = self.db.collection::<Document>(collection);

        match command_name {
            "find_one" => {
                let filter = command_doc(&command, "filter")?.unwrap_or_default();
                let mut action = docs.find_one(filter);
                if let Some(sort) = command_doc(&command, "sort")? {
                    action = action.sort(sort);
                }

                let doc = action.await.map_err(|err| err.to_string())?;
                Ok(doc
                    .map(mongo_doc_to_json)
                    .map(|value| vec![value])
                    .unwrap_or_default())
            }
            "find" => {
                let filter = command_doc(&command, "filter")?.unwrap_or_default();
                let mut action = docs.find(filter);
                if let Some(sort) = command_doc(&command, "sort")? {
                    action = action.sort(sort);
                }
                if let Some(limit) = command.get("limit").and_then(|value| value.as_i64()) {
                    if limit > 0 {
                        action = action.limit(limit);
                    }
                }

                let mut cursor = action.await.map_err(|err| err.to_string())?;
                let mut rows = Vec::new();
                while let Some(doc) = cursor.try_next().await.map_err(|err| err.to_string())? {
                    rows.push(mongo_doc_to_json(doc));
                }
                Ok(rows)
            }
            "insert_one" => {
                let document = command_doc(&command, "document")?
                    .ok_or_else(|| "document is required".to_string())?;
                let result = docs
                    .insert_one(document)
                    .await
                    .map_err(|err| err.to_string())?;
                Ok(vec![json!({
                    "inserted_id": mongo_bson_to_json(result.inserted_id),
                })])
            }
            "update_one" => {
                let filter = command_doc(&command, "filter")?.unwrap_or_default();
                let update = command_doc(&command, "update")?
                    .ok_or_else(|| "update is required".to_string())?;
                let mut action = docs.update_one(filter, update);
                if let Some(upsert) = command.get("upsert").and_then(|value| value.as_bool()) {
                    action = action.upsert(upsert);
                }

                let result = action.await.map_err(|err| err.to_string())?;
                Ok(vec![json!({
                    "matched_count": result.matched_count,
                    "modified_count": result.modified_count,
                    "upserted_id": result.upserted_id.map(mongo_bson_to_json),
                })])
            }
            "delete_one" => {
                let filter = command_doc(&command, "filter")?.unwrap_or_default();
                let result = docs
                    .delete_one(filter)
                    .await
                    .map_err(|err| err.to_string())?;
                Ok(vec![json!({
                    "deleted_count": result.deleted_count,
                })])
            }
            _ => Err(format!("unsupported doc op: {command_name}")),
        }
    }

    async fn try_begin_task(
        &self,
        task_id: &str,
        task_type: &str,
        attempt: u64,
    ) -> Result<Option<Value>, String> {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let tasks = self.db.collection::<Document>("task_idempotency");
        let insert_result = tasks
            .insert_one(doc! {
                "_id": task_id,
                "task_type": task_type,
                "status": "running",
                "attempt": attempt as i64,
                "result": Bson::Null,
                "created_at": now_ms,
                "updated_at": now_ms,
            })
            .await;

        if insert_result.is_ok() {
            return Ok(None);
        }

        let err_text = insert_result
            .err()
            .map(|err| err.to_string())
            .unwrap_or_default();
        if !err_text.contains("E11000") {
            return Err(err_text);
        }

        let stale_before = now_ms - (TASK_RUNNING_STALE_SECS as i64 * 1000);
        let reclaim_result = tasks
            .update_one(
                doc! {
                    "_id": task_id,
                    "status": "running",
                    "attempt": { "$lte": attempt as i64 },
                    "updated_at": { "$lt": stale_before },
                },
                doc! {
                    "$set": {
                        "task_type": task_type,
                        "status": "running",
                        "attempt": attempt as i64,
                        "result": Bson::Null,
                        "updated_at": now_ms,
                    }
                },
            )
            .await
            .map_err(|err| err.to_string())?;
        if reclaim_result.modified_count > 0 {
            return Ok(None);
        }

        let task_doc = tasks
            .find_one(doc! { "_id": task_id })
            .await
            .map_err(|err| err.to_string())?
            .ok_or_else(|| "task_idempotency record not found".to_string())?;
        if task_doc.get_str("status").unwrap_or("") == "done" {
            let result = task_doc
                .get("result")
                .cloned()
                .and_then(|value| mongodb::bson::from_bson::<Value>(value).ok())
                .unwrap_or(Value::Null);
            return Ok(Some(result));
        }

        Err("task_running".to_string())
    }

    async fn finish_task(&self, task_id: &str, result: &Value) -> Result<(), String> {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let result_bson = mongodb::bson::to_bson(result).map_err(|err| err.to_string())?;
        self.db
            .collection::<Document>("task_idempotency")
            .update_one(
                doc! { "_id": task_id },
                doc! {
                    "$set": {
                        "status": "done",
                        "result": result_bson,
                        "updated_at": now_ms,
                    }
                },
            )
            .await
            .map_err(|err| err.to_string())?;
        Ok(())
    }
}

fn command_doc(command: &Value, key: &str) -> Result<Option<Document>, String> {
    let Some(value) = command.get(key) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    mongodb::bson::to_document(value)
        .map(Some)
        .map_err(|err| format!("{key} must be an object: {err}"))
}

fn mongo_doc_to_json(doc: Document) -> Value {
    mongo_bson_to_json(Bson::Document(doc))
}

fn mongo_bson_to_json(value: Bson) -> Value {
    mongodb::bson::from_bson::<Value>(value).unwrap_or(Value::Null)
}

async fn migrate_legacy_players(db: &Database) -> Result<(), String> {
    let players = db.collection::<Document>("players");
    let mut cursor = players.find(doc! {}).await.map_err(|err| err.to_string())?;

    while let Some(player) = cursor.try_next().await.map_err(|err| err.to_string())? {
        let userid = player.get_str("userid").unwrap_or("").trim().to_string();
        if userid.is_empty() {
            continue;
        }

        let nickname = player.get_str("nickname").unwrap_or("player").to_string();
        let now_ms = chrono::Utc::now().timestamp_millis();

        let profile_filter = doc! { "userid": &userid };
        let exists_profile = db
            .collection::<Document>("player_profiles")
            .find_one(profile_filter.clone())
            .await
            .map_err(|err| err.to_string())?
            .is_some();
        if !exists_profile {
            db.collection::<Document>("player_profiles")
                .insert_one(doc! {
                    "userid": &userid,
                    "nickname": nickname,
                    "level": 1_i32,
                    "exp": 0_i64,
                    "avatar_id": 0_i32,
                    "created_at": now_ms,
                    "updated_at": now_ms,
                })
                .await
                .map_err(|err| err.to_string())?;
        }

        let exists_wallet = db
            .collection::<Document>("player_wallets")
            .find_one(doc! { "userid": &userid })
            .await
            .map_err(|err| err.to_string())?
            .is_some();
        if !exists_wallet {
            db.collection::<Document>("player_wallets")
                .insert_one(doc! {
                    "userid": &userid,
                    "gold": 0_i64,
                    "diamond": 0_i64,
                    "stamina": 100_i32,
                    "updated_at": now_ms,
                })
                .await
                .map_err(|err| err.to_string())?;
        }
    }

    let _ = players.drop().await;
    Ok(())
}
