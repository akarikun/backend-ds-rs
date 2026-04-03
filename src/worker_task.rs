use crate::commons::db;
use futures::FutureExt;
use futures::future::BoxFuture;
use serde_json::{Value, json};
use std::collections::HashMap;

type TaskHandler = fn(Value) -> BoxFuture<'static, Value>;

lazy_static::lazy_static! {
    static ref TASK_METHODS: HashMap<&'static str, TaskHandler> = {
        let mut methods: HashMap<&'static str, TaskHandler> = HashMap::new();
        methods.insert("create_player", create_player_task as TaskHandler);
        methods
    };
}

pub async fn run_worker_task(task: Value) -> Value {
    let task_type = task.get("type").and_then(|value| value.as_str()).unwrap_or("");
    if let Some(handler) = TASK_METHODS.get(task_type) {
        return handler(task).await;
    }

    json!({
        "ok": false,
        "error": "unknown_task_type",
        "type": task_type,
        "task": task,
    })
}

fn create_player_task(task: Value) -> BoxFuture<'static, Value> {
    async move {
        let userid = task.get("userid").and_then(|value| value.as_str()).unwrap_or("");
        let nickname = task
            .get("nickname")
            .and_then(|value| value.as_str())
            .unwrap_or("player");

        if userid.is_empty() {
            return json!({
                "ok": false,
                "type": "create_player",
                "error": "userid is empty",
            });
        }

        match db::create_player_info(userid, nickname).await {
            Ok(player) => json!({
                "ok": true,
                "type": "create_player",
                "player": player,
            }),
            Err(err) => json!({
                "ok": false,
                "type": "create_player",
                "error": err,
            }),
        }
    }
    .boxed()
}
