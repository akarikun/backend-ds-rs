use crate::addons_runtime;
use crate::commons::db;
use futures::future::BoxFuture;
use serde_json::{Value, json};
use std::collections::HashMap;
use tokio::time::{Duration, sleep};

const IDEMPOTENCY_WAIT_POLL_MS: u64 = 200;
const IDEMPOTENCY_WAIT_TIMEOUT_MS: u64 = 10_000;

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
        .unwrap_or("")
        .to_string();
    let task_id = task
        .get("task_id")
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .to_string();
    let attempt = task
        .get("attempt")
        .and_then(|value| value.as_u64())
        .unwrap_or(1)
        .max(1);

    if task_id.is_empty() {
        return run_worker_task_inner(task).await;
    }

    match db::try_begin_task(task_id.as_str(), task_type.as_str(), attempt).await {
        Ok(None) => {
            let result = run_worker_task_inner(task).await;
            if let Err(err) = db::finish_task(task_id.as_str(), &result).await {
                return json!({
                    "ok": false,
                    "error": "finish_task_idempotency_failed",
                    "detail": err,
                    "task_id": task_id,
                    "type": task_type,
                });
            }
            result
        }
        Ok(Some(result)) => result,
        Err(err) if err == "task_running" => {
            wait_running_task_result(task_id.as_str(), task_type.as_str(), attempt).await
        }
        Err(err) => json!({
            "ok": false,
            "error": err,
            "task_id": task_id,
            "type": task_type,
            "attempt": attempt,
        }),
    }
}

async fn wait_running_task_result(task_id: &str, task_type: &str, attempt: u64) -> Value {
    let max_wait_rounds = (IDEMPOTENCY_WAIT_TIMEOUT_MS / IDEMPOTENCY_WAIT_POLL_MS).max(1);

    for _ in 0..max_wait_rounds {
        sleep(Duration::from_millis(IDEMPOTENCY_WAIT_POLL_MS)).await;

        match db::try_begin_task(task_id, task_type, attempt).await {
            Ok(Some(result)) => return result,
            Err(err) if err == "task_running" => {}
            Err(err) => {
                return json!({
                    "ok": false,
                    "error": err,
                    "task_id": task_id,
                    "type": task_type,
                    "attempt": attempt,
                });
            }
            Ok(None) => {
                return json!({
                    "ok": false,
                    "error": "task_idempotency_state_lost",
                    "task_id": task_id,
                    "type": task_type,
                    "attempt": attempt,
                });
            }
        }
    }

    json!({
        "ok": false,
        "error": "task_running_wait_timeout",
        "task_id": task_id,
        "type": task_type,
        "attempt": attempt,
    })
}

async fn run_worker_task_inner(task: Value) -> Value {
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
