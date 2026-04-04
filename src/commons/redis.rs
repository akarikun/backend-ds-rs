use crate::commons::utils::CONFIG as config;
use redis::AsyncCommands;
use redis::aio::MultiplexedConnection;
use serde_json::Value;
use tokio::sync::OnceCell;

static REDIS_CONN: OnceCell<Option<MultiplexedConnection>> = OnceCell::const_new();

pub async fn init_redis() -> Result<(), String> {
    REDIS_CONN
        .get_or_try_init(|| async {
            if !config.redis.enabled {
                return Ok::<Option<MultiplexedConnection>, String>(None);
            }

            let client = redis::Client::open(config.redis.redis_url.as_str())
                .map_err(|err| err.to_string())?;
            let conn = client
                .get_multiplexed_async_connection()
                .await
                .map_err(|err| err.to_string())?;
            Ok(Some(conn))
        })
        .await?;

    Ok(())
}

pub async fn redis_get(key: &str) -> Result<Option<Value>, String> {
    let Some(mut conn) = redis_conn() else {
        return Ok(None);
    };

    let key = redis_key(key);
    let text: Option<String> = conn.get(key).await.map_err(|err| err.to_string())?;
    let Some(text) = text else {
        return Ok(None);
    };

    let value = serde_json::from_str(&text).map_err(|err| err.to_string())?;
    Ok(Some(value))
}

pub async fn redis_set(key: &str, value: &Value, ttl_secs: Option<u64>) -> Result<(), String> {
    let Some(mut conn) = redis_conn() else {
        return Ok(());
    };

    let key = redis_key(key);
    let text = serde_json::to_string(value).map_err(|err| err.to_string())?;
    let ttl_secs = ttl_secs
        .unwrap_or(config.redis.default_ttl_secs)
        .max(1);
    let _: () = conn
        .set_ex(key, text, ttl_secs)
        .await
        .map_err(|err| err.to_string())?;
    Ok(())
}

pub async fn redis_del(key: &str) -> Result<(), String> {
    let Some(mut conn) = redis_conn() else {
        return Ok(());
    };

    let key = redis_key(key);
    let _: usize = conn.del(key).await.map_err(|err| err.to_string())?;
    Ok(())
}

fn redis_conn() -> Option<MultiplexedConnection> {
    REDIS_CONN.get().and_then(|conn| conn.clone())
}

fn redis_key(key: &str) -> String {
    format!("{}:{}", config.redis.key_prefix, key)
}
