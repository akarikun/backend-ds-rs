use crate::commons::db::DbDriver;
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
        Err("db.query only supports postgresql".to_string())
    }
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
