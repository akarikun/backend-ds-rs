use crate::addons_runtime;
use futures::future::BoxFuture;
use serde_json::{Value, json};
use std::collections::HashMap;

type TaskHandler = fn(Value) -> BoxFuture<'static, Value>;

lazy_static::lazy_static! {
    static ref TASK_METHODS: HashMap<&'static str, TaskHandler> = {
        HashMap::new()
    };
}

pub fn has_task_handler(task_type: &str) -> bool {
    TASK_METHODS.contains_key(task_type) || (!task_type.is_empty() && addons_runtime::has_addons())
}

pub async fn run_client_task(message: Value) -> Option<(String, Value)> {
    let task_type = message
        .get("cmd")
        .or_else(|| message.get("type"))
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .to_string();

    if !has_task_handler(&task_type) {
        return None;
    }

    let mut task = match message.get("data").cloned() {
        Some(Value::Object(_)) => message.get("data").cloned().unwrap_or_else(|| json!({})),
        Some(data) => json!({ "data": data }),
        None => message.clone(),
    };
    task["type"] = json!(task_type);

    let result = run_worker_task(task).await;
    Some((format!("{task_type}_result"), result))
}

pub async fn run_worker_task(task: Value) -> Value {
    let task_type = task
        .get("type")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    if let Some(handler) = TASK_METHODS.get(task_type) {
        return handler(task).await;
    }

    if let Some(result) = addons_runtime::run_addon_task(task_type, &task) {
        return result;
    }

    json!({
        "ok": false,
        "error": "unknown_task_type",
        "type": task_type,
        "task": task,
    })
}
