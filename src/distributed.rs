use crate::commons::utils::CONFIG as config;
use crate::worker_task;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use futures::FutureExt;
use rust_socketio::Payload;
use rust_socketio::asynchronous::{Client, ClientBuilder};
use serde_json::{Value, json};
use socketioxide::SocketIo;
use socketioxide::extract::{Data, SocketRef};
use socketioxide::socket::DisconnectReason;
use std::fs;
use std::sync::LazyLock;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{Duration, sleep};

#[derive(Clone, Debug)]
struct ServerNode {
    node_id: String,
    addr: String,
    role: String,
    device_info: Value,
    socket: SocketRef,
    current_load: u64,
    max_load: u64,
    last_heartbeat: u64,
}

#[derive(Clone, Debug)]
struct PendingTask {
    requester_socket: SocketRef,
    task: Value,
    task_cost: u64,
    worker_id: String,
    attempt: u64,
    retries_left: u64,
    timeout_secs: u64,
}

lazy_static::lazy_static! {
    static ref SERVER_NODES: DashMap<String, ServerNode> = DashMap::new();
    static ref PENDING_TASKS: DashMap<String, PendingTask> = DashMap::new();
    static ref MASTER_DASHBOARD: RwLock<Value> = RwLock::new(empty_dashboard_snapshot());
    static ref WORKER_CURRENT_LOAD: AtomicU64 = AtomicU64::new(0);
    static ref MASTER_TASK_SEQ: AtomicU64 = AtomicU64::new(1);
}

static CURRENT_NODE_DEVICE_INFO: LazyLock<Value> = LazyLock::new(build_current_node_device_info);

pub fn current_node_device_info() -> Value {
    let mut device_info = CURRENT_NODE_DEVICE_INFO.clone();
    let (used_memory_kb, memory_used_ratio) = memory_usage();
    device_info["used_memory_kb"] = json!(used_memory_kb);
    device_info["memory_used_ratio"] = json!(memory_used_ratio);
    device_info
}

pub fn start_worker_client() {
    if config.distributed.node_role != "worker" {
        return;
    }

    tokio::spawn(async {
        connect_master_loop().await;
    });
}

fn now_ts() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn parse_message(data: &Data<Value>) -> Value {
    if let Ok(text) = serde_json::from_value::<String>(data.0.clone()) {
        return serde_json::from_str::<Value>(&text).unwrap_or_else(|_| json!({}));
    }
    data.0.clone()
}

fn payload_master_id(payload: &Payload) -> String {
    match payload {
        Payload::Text(values) => values
            .first()
            .and_then(|value| value.get("from"))
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .to_string(),
        _ => String::new(),
    }
}

fn payload_task_cost(payload: &Payload) -> u64 {
    match payload {
        Payload::Text(values) => values
            .first()
            .and_then(|value| value.get("task_cost"))
            .and_then(|value| value.as_u64())
            .unwrap_or(1)
            .max(1),
        _ => 1,
    }
}

fn payload_task_id(payload: &Payload) -> String {
    match payload {
        Payload::Text(values) => values
            .first()
            .and_then(|value| value.get("task_id"))
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .to_string(),
        _ => String::new(),
    }
}

fn payload_task_attempt(payload: &Payload) -> u64 {
    match payload {
        Payload::Text(values) => values
            .first()
            .and_then(|value| value.get("attempt"))
            .and_then(|value| value.as_u64())
            .unwrap_or(1)
            .max(1),
        _ => 1,
    }
}

fn payload_task(payload: &Payload) -> Value {
    match payload {
        Payload::Text(values) => values
            .first()
            .and_then(|value| value.get("task"))
            .cloned()
            .unwrap_or_else(|| json!({})),
        _ => json!({}),
    }
}

fn empty_dashboard_snapshot() -> Value {
    json!({
        "worker_count": 0,
        "workers": [],
        "all_nodes": {
            "count": 0,
            "nodes": [],
        },
    })
}

fn save_master_dashboard(snapshot: Value) {
    if let Ok(mut dashboard) = MASTER_DASHBOARD.write() {
        *dashboard = snapshot;
    }
}

fn sync_master_dashboard_from_nodes(all_nodes: Value) {
    let workers = all_nodes
        .get("nodes")
        .and_then(|nodes| nodes.as_array())
        .map(|nodes| {
            nodes
                .iter()
                .filter(|node| node.get("role").and_then(|role| role.as_str()) == Some("worker"))
                .map(|node| {
                    json!({
                        "node_id": node.get("node_id").cloned().unwrap_or_else(|| json!("")),
                        "addr": node.get("addr").cloned().unwrap_or_else(|| json!("")),
                        "current_load": node
                            .get("current_load")
                            .cloned()
                            .unwrap_or_else(|| json!(0)),
                        "max_load": node.get("max_load").cloned().unwrap_or_else(|| json!(0)),
                        "last_heartbeat": node
                            .get("last_heartbeat")
                            .cloned()
                            .unwrap_or_else(|| json!(0)),
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    save_master_dashboard(json!({
        "worker_count": workers.len(),
        "workers": workers,
        "all_nodes": all_nodes,
    }));
}

fn sync_master_dashboard_from_payload(payload: &Payload) {
    if let Payload::Text(values) = payload {
        if let Some(value) = values.first() {
            if let Some(nodes) = value.get("nodes") {
                if nodes.is_object() {
                    // register_ok: { node_id, ts, nodes: { count, nodes: [...] } }
                    sync_master_dashboard_from_nodes(nodes.clone());
                } else {
                    // server_nodes: { count, nodes: [...] }
                    sync_master_dashboard_from_nodes(value.clone());
                }
            } else {
                sync_master_dashboard_from_nodes(value.clone());
            }
        }
    }
}

fn valid_server_token(data: &Value) -> bool {
    data.get("server_token")
        .and_then(|v| v.as_str())
        .is_some_and(|token| token == config.distributed.server_token)
}

fn master_url() -> String {
    let addr = config.distributed.master_addr.trim();
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    }
}

fn worker_auth_payload() -> Value {
    json!({
        "server_token": config.distributed.server_token,
        "node_id": config.distributed.node_id,
        "addr": config.distributed.node_addr,
        "role": config.distributed.node_role,
        "device_info": current_node_device_info(),
        "current_load": 0,
        "max_load": config.distributed.max_load,
    })
}

async fn connect_master_loop() {
    loop {
        match connect_master_once().await {
            Ok(client) => {
                run_worker_heartbeat(client).await;
            }
            Err(err) => {
                dbg!("connect_master_failed", err.to_string());
                sleep(Duration::from_secs(
                    config.distributed.heartbeat_interval_secs.max(1),
                ))
                .await;
            }
        }
    }
}

async fn connect_master_once() -> Result<Client, rust_socketio::Error> {
    ClientBuilder::new(master_url())
        .namespace(config.distributed.server_ns.as_str())
        .auth(worker_auth_payload())
        .reconnect(true)
        .reconnect_on_disconnect(true)
        .on("register_ok", |payload: Payload, _client: Client| {
            async move {
                sync_master_dashboard_from_payload(&payload);
                dbg!("worker_register_ok", payload);
            }
            .boxed()
        })
        .on("server_nodes", |payload: Payload, _client: Client| {
            async move {
                sync_master_dashboard_from_payload(&payload);
            }
            .boxed()
        })
        .on("task", |payload: Payload, client: Client| {
            async move {
                let master_id = payload_master_id(&payload);
                let task_id = payload_task_id(&payload);
                let attempt = payload_task_attempt(&payload);
                let task_cost = payload_task_cost(&payload);
                let task = attach_task_metadata(payload_task(&payload), &task_id, attempt);
                WORKER_CURRENT_LOAD.fetch_add(task_cost, Ordering::Relaxed);
                let task_result = worker_task::run_worker_task(task).await;
                let result = json!({
                    "cmd": "task_result",
                    "master_id": master_id,
                    "task_id": task_id,
                    "attempt": attempt,
                    "task_cost": task_cost,
                    "result": task_result,
                });
                _ = client.emit("server_msg", result).await;
                WORKER_CURRENT_LOAD.fetch_sub(task_cost, Ordering::Relaxed);
            }
            .boxed()
        })
        .on("disconnect", |payload: Payload, _client: Client| {
            async move {
                save_master_dashboard(empty_dashboard_snapshot());
                dbg!("worker_master_disconnect", payload);
            }
            .boxed()
        })
        .on("error", |payload: Payload, _client: Client| {
            async move {
                dbg!("worker_master_error", payload);
            }
            .boxed()
        })
        .connect()
        .await
}

async fn run_worker_heartbeat(client: Client) {
    let interval_secs = config.distributed.heartbeat_interval_secs.max(1);

    loop {
        let heartbeat = json!({
            "cmd": "heartbeat",
        });
        if client.emit("server_msg", heartbeat).await.is_err() {
            return;
        }

        let load = json!({
            "cmd": "report_load",
            "current_load": WORKER_CURRENT_LOAD.load(Ordering::Relaxed),
            "max_load": config.distributed.max_load,
        });
        if client.emit("server_msg", load).await.is_err() {
            return;
        }

        if client
            .emit("server_msg", json!({ "cmd": "list_nodes" }))
            .await
            .is_err()
        {
            return;
        }

        sleep(Duration::from_secs(interval_secs)).await;
    }
}

fn node_snapshot() -> Value {
    prune_expired_nodes();

    let mut nodes = SERVER_NODES
        .iter()
        .map(|entry| {
            let node = entry.value();
            json!({
                "node_id": node.node_id,
                "addr": node.addr,
                "role": node.role,
                "device_info": node.device_info,
                "current_load": node.current_load,
                "max_load": node.max_load,
                "last_heartbeat": node.last_heartbeat,
            })
        })
        .collect::<Vec<_>>();

    let self_node_id = config.distributed.node_id.clone();
    if !nodes.iter().any(|node| {
        node.get("node_id").and_then(|value| value.as_str()) == Some(self_node_id.as_str())
    }) {
        nodes.push(json!({
            "node_id": self_node_id,
            "addr": config.distributed.node_addr,
            "role": config.distributed.node_role,
            "device_info": current_node_device_info(),
            "current_load": if config.distributed.node_role == "worker" {
                WORKER_CURRENT_LOAD.load(Ordering::Relaxed)
            } else {
                0
            },
            "max_load": config.distributed.max_load,
            "last_heartbeat": now_ts(),
        }));
    }

    json!({
        "count": nodes.len(),
        "nodes": nodes,
    })
}

pub fn dashboard_snapshot() -> Value {
    if config.distributed.node_role == "worker" {
        let mut dashboard = MASTER_DASHBOARD
            .read()
            .map(|dashboard| dashboard.clone())
            .unwrap_or_else(|_| empty_dashboard_snapshot());

        let self_node_id = config.distributed.node_id.clone();
        let self_node = json!({
            "node_id": self_node_id,
            "addr": config.distributed.node_addr,
            "role": config.distributed.node_role,
            "device_info": current_node_device_info(),
            "current_load": WORKER_CURRENT_LOAD.load(Ordering::Relaxed),
            "max_load": config.distributed.max_load,
            "last_heartbeat": now_ts(),
        });

        if let Some(workers) = dashboard
            .get_mut("workers")
            .and_then(|value| value.as_array_mut())
        {
            if !workers.iter().any(|worker| {
                worker.get("node_id").and_then(|value| value.as_str()) == Some(&self_node_id)
            }) {
                workers.push(self_node);
            }
            dashboard["worker_count"] = json!(workers.len());
        }

        return dashboard;
    }

    prune_expired_nodes();

    let all_nodes = node_snapshot();

    let workers = all_nodes
        .get("nodes")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|node| node.get("role").and_then(|role| role.as_str()) == Some("worker"))
        .map(|node| {
            json!({
                "node_id": node.get("node_id").cloned().unwrap_or_else(|| json!("")),
                "addr": node.get("addr").cloned().unwrap_or_else(|| json!("")),
                "current_load": node.get("current_load").cloned().unwrap_or_else(|| json!(0)),
                "max_load": node.get("max_load").cloned().unwrap_or_else(|| json!(0)),
                "last_heartbeat": node.get("last_heartbeat").cloned().unwrap_or_else(|| json!(0)),
            })
        })
        .collect::<Vec<_>>();

    json!({
        "worker_count": workers.len(),
        "workers": workers,
        "all_nodes": all_nodes,
    })
}

fn current_node_id(socket: &SocketRef) -> Option<String> {
    socket
        .extensions
        .get::<String>()
        .as_ref()
        .map(|node_id| node_id.to_string())
}

fn task_cost_of(msg: &Value) -> u64 {
    msg.get("task_cost")
        .and_then(|value| value.as_u64())
        .or_else(|| {
            msg.get("task")
                .and_then(|task| task.get("task_cost"))
                .and_then(|value| value.as_u64())
        })
        .unwrap_or(1)
        .max(1)
}

fn task_id_of(msg: &Value) -> String {
    msg.get("task_id")
        .and_then(|value| value.as_str())
        .filter(|task_id| !task_id.is_empty())
        .map(|task_id| task_id.to_string())
        .unwrap_or_else(|| {
            let seq = MASTER_TASK_SEQ.fetch_add(1, Ordering::Relaxed);
            format!("{}-{}-{seq}", config.distributed.node_id, now_ts())
        })
}

fn task_attempt_of(msg: &Value) -> u64 {
    msg.get("attempt")
        .and_then(|value| value.as_u64())
        .unwrap_or(1)
        .max(1)
}

fn task_timeout_secs_of(msg: &Value) -> u64 {
    msg.get("timeout_secs")
        .and_then(|value| value.as_u64())
        .unwrap_or(config.distributed.node_timeout_secs.max(1))
        .max(1)
}

fn task_retries_of(msg: &Value) -> u64 {
    msg.get("max_retries")
        .and_then(|value| value.as_u64())
        .unwrap_or(1)
}

fn reserve_worker_load(task_cost: u64) -> Option<ServerNode> {
    prune_expired_nodes();

    let worker_id = SERVER_NODES
        .iter()
        .filter(|entry| {
            let node = entry.value();
            node.role == "worker" && node.current_load.saturating_add(task_cost) <= node.max_load
        })
        .min_by_key(|entry| entry.value().current_load)
        .map(|entry| entry.value().node_id.clone())?;

    if let Some(mut node) = SERVER_NODES.get_mut(&worker_id) {
        if node.current_load.saturating_add(task_cost) <= node.max_load {
            node.current_load += task_cost;
            return Some(node.clone());
        }
    }

    None
}

fn release_worker_load(worker_id: &str, task_cost: u64) {
    if let Some(mut node) = SERVER_NODES.get_mut(worker_id) {
        node.current_load = node.current_load.saturating_sub(task_cost.max(1));
    }
}

fn is_node_expired(node: &ServerNode, now: u64) -> bool {
    let timeout_secs = config.distributed.node_timeout_secs;
    timeout_secs > 0 && now.saturating_sub(node.last_heartbeat) > timeout_secs
}

fn prune_expired_nodes() {
    let now = now_ts();
    let expired_nodes = SERVER_NODES
        .iter()
        .filter(|entry| is_node_expired(entry.value(), now))
        .map(|entry| entry.value().node_id.clone())
        .collect::<Vec<_>>();

    for node_id in expired_nodes {
        if let Some((_, node)) = SERVER_NODES.remove(&node_id) {
            _ = node.socket.disconnect();
        }
    }
}

fn build_worker_task_message(
    master_id: String,
    worker_id: String,
    task_id: String,
    attempt: u64,
    task_cost: u64,
    task: Value,
) -> Value {
    json!({
        "from": master_id,
        "worker_id": worker_id,
        "task_id": task_id,
        "attempt": attempt,
        "task_cost": task_cost,
        "task": task,
        "ts": now_ts(),
    })
}

fn attach_task_metadata(mut task: Value, task_id: &str, attempt: u64) -> Value {
    if !task_id.is_empty() {
        task["task_id"] = json!(task_id);
    }
    task["attempt"] = json!(attempt.max(1));
    task
}

fn build_task_timeout_result(
    task_id: &str,
    attempt: u64,
    worker_id: &str,
    task_cost: u64,
) -> Value {
    json!({
        "ok": false,
        "error": "task_timeout",
        "task_id": task_id,
        "attempt": attempt,
        "worker_id": worker_id,
        "task_cost": task_cost,
        "ts": now_ts(),
    })
}

fn watch_task_timeout(task_id: String, attempt: u64, timeout_secs: u64) {
    tokio::spawn(async move {
        sleep(Duration::from_secs(timeout_secs)).await;
        retry_or_fail_timeout_task(task_id, attempt).await;
    });
}

async fn retry_or_fail_timeout_task(task_id: String, attempt: u64) {
    let Some(mut pending) = PENDING_TASKS.get_mut(&task_id) else {
        return;
    };

    if pending.attempt != attempt {
        return;
    }

    let requester_socket = pending.requester_socket.clone();
    let task = pending.task.clone();
    let task_cost = pending.task_cost;
    let timeout_secs = pending.timeout_secs;
    let old_worker_id = pending.worker_id.clone();

    if pending.retries_left == 0 {
        drop(pending);
        PENDING_TASKS.remove(&task_id);
        _ = requester_socket.emit(
            "task_result",
            &build_task_timeout_result(&task_id, attempt, &old_worker_id, task_cost),
        );
        return;
    }

    pending.attempt += 1;
    pending.retries_left = pending.retries_left.saturating_sub(1);
    let next_attempt = pending.attempt;
    drop(pending);

    let Some(worker) = reserve_worker_load(task_cost) else {
        PENDING_TASKS.remove(&task_id);
        _ = requester_socket.emit(
            "task_result",
            &json!({
                "ok": false,
                "error": "task_retry_no_available_worker",
                "task_id": task_id,
                "attempt": next_attempt,
                "worker_id": old_worker_id,
                "task_cost": task_cost,
                "ts": now_ts(),
            }),
        );
        return;
    };

    let worker_id = worker.node_id.clone();
    let retry_message = build_worker_task_message(
        config.distributed.node_id.clone(),
        worker_id.clone(),
        task_id.clone(),
        next_attempt,
        task_cost,
        task,
    );

    if worker.socket.emit("task", &retry_message).is_err() {
        release_worker_load(&worker_id, task_cost);
        PENDING_TASKS.remove(&task_id);
        _ = requester_socket.emit(
            "task_result",
            &json!({
                "ok": false,
                "error": "task_retry_emit_failed",
                "task_id": task_id,
                "attempt": next_attempt,
                "worker_id": worker_id,
                "task_cost": task_cost,
                "ts": now_ts(),
            }),
        );
        return;
    }

    if let Some(mut refreshed) = PENDING_TASKS.get_mut(&task_id) {
        if refreshed.attempt == next_attempt {
            refreshed.worker_id = worker_id;
        }
    }

    watch_task_timeout(task_id, next_attempt, timeout_secs);
}

fn should_master_run_task_locally(task: &Value) -> bool {
    matches!(
        task.get("type")
            .and_then(|value| value.as_str())
            .unwrap_or(""),
        "create_player"
    )
}

async fn run_task_on_master(task: Value, task_id: &str, attempt: u64, task_cost: u64) -> Value {
    WORKER_CURRENT_LOAD.fetch_add(task_cost, Ordering::Relaxed);
    let task = attach_task_metadata(task, task_id, attempt);
    let result = worker_task::run_worker_task(task).await;
    WORKER_CURRENT_LOAD.fetch_sub(task_cost, Ordering::Relaxed);
    result
}

async fn report_load(socket: &SocketRef, msg: &Value) {
    let Some(node_id) = current_node_id(socket) else {
        return;
    };

    if let Some(mut node) = SERVER_NODES.get_mut(&node_id) {
        if let Some(current_load) = msg.get("current_load").and_then(|v| v.as_u64()) {
            node.current_load = current_load;
        }
        if let Some(max_load) = msg.get("max_load").and_then(|v| v.as_u64()) {
            node.max_load = max_load.max(1);
        }
        node.last_heartbeat = now_ts();
    }

    _ = socket.emit(
        "load_ack",
        &json!({
            "node_id": node_id,
            "ts": now_ts(),
        }),
    );
}

async fn dispatch_task(socket: &SocketRef, msg: &Value) {
    let Some(task) = msg.get("task").cloned() else {
        _ = socket.emit(
            "dispatch_result",
            &json!({
                "ok": false,
                "error": "missing_task",
            }),
        );
        return;
    };

    let task_id = task_id_of(msg);
    if PENDING_TASKS.contains_key(&task_id) {
        _ = socket.emit(
            "dispatch_result",
            &json!({
                "ok": false,
                "error": "duplicate_task_id",
                "task_id": task_id,
                "ts": now_ts(),
            }),
        );
        return;
    }

    let task_cost = task_cost_of(msg);
    let attempt = task_attempt_of(msg);
    let timeout_secs = task_timeout_secs_of(msg);
    let retries_left = task_retries_of(msg);

    if should_master_run_task_locally(&task) {
        _ = socket.emit(
            "dispatch_result",
            &json!({
                "ok": true,
                "task_id": task_id,
                "attempt": attempt,
                "worker_id": config.distributed.node_id,
                "task_cost": task_cost,
                "local_exec": true,
                "ts": now_ts(),
            }),
        );

        let result = run_task_on_master(task, &task_id, attempt, task_cost).await;
        _ = socket.emit(
            "task_result",
            &json!({
                "task_id": task_id,
                "attempt": attempt,
                "worker_id": config.distributed.node_id,
                "task_cost": task_cost,
                "result": result,
                "local_exec": true,
                "ts": now_ts(),
            }),
        );
        return;
    }

    let Some(worker) = reserve_worker_load(task_cost) else {
        _ = socket.emit(
            "dispatch_result",
            &json!({
                "ok": true,
                "task_id": task_id,
                "attempt": attempt,
                "worker_id": config.distributed.node_id,
                "task_cost": task_cost,
                "local_exec": true,
                "fallback_reason": "no_available_worker",
                "ts": now_ts(),
            }),
        );

        let result = run_task_on_master(task, &task_id, attempt, task_cost).await;
        _ = socket.emit(
            "task_result",
            &json!({
                "task_id": task_id,
                "attempt": attempt,
                "worker_id": config.distributed.node_id,
                "task_cost": task_cost,
                "result": result,
                "local_exec": true,
                "fallback_reason": "no_available_worker",
                "ts": now_ts(),
            }),
        );
        return;
    };

    let worker_id = worker.node_id.clone();
    PENDING_TASKS.insert(
        task_id.clone(),
        PendingTask {
            requester_socket: socket.clone(),
            task: task.clone(),
            task_cost,
            worker_id: worker_id.clone(),
            attempt,
            retries_left,
            timeout_secs,
        },
    );

    let task_message = build_worker_task_message(
        current_node_id(socket).unwrap_or_default(),
        worker_id.clone(),
        task_id.clone(),
        attempt,
        task_cost,
        task.clone(),
    );
    if worker.socket.emit("task", &task_message).is_err() {
        PENDING_TASKS.remove(&task_id);
        release_worker_load(&worker_id, task_cost);

        _ = socket.emit(
            "dispatch_result",
            &json!({
                "ok": true,
                "task_id": task_id,
                "attempt": attempt,
                "worker_id": config.distributed.node_id,
                "task_cost": task_cost,
                "local_exec": true,
                "fallback_reason": "worker_emit_failed",
                "ts": now_ts(),
            }),
        );

        let result = run_task_on_master(task, &task_id, attempt, task_cost).await;
        _ = socket.emit(
            "task_result",
            &json!({
                "task_id": task_id,
                "attempt": attempt,
                "worker_id": config.distributed.node_id,
                "task_cost": task_cost,
                "result": result,
                "local_exec": true,
                "fallback_reason": "worker_emit_failed",
                "ts": now_ts(),
            }),
        );
        return;
    }

    watch_task_timeout(task_id.clone(), attempt, timeout_secs);

    _ = socket.emit(
        "dispatch_result",
        &json!({
            "ok": true,
            "task_id": task_id,
            "attempt": attempt,
            "worker_id": worker_id,
            "task_cost": task_cost,
            "timeout_secs": timeout_secs,
            "retries_left": retries_left,
            "ts": now_ts(),
        }),
    );
}

async fn forward_task_result(socket: &SocketRef, msg: &Value) {
    let worker_id = current_node_id(socket).unwrap_or_default();
    let task_cost = task_cost_of(msg);
    release_worker_load(&worker_id, task_cost);

    let task_id = msg
        .get("task_id")
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .to_string();
    let attempt = task_attempt_of(msg);

    if !task_id.is_empty() {
        let requester_socket = match PENDING_TASKS.entry(task_id.clone()) {
            Entry::Occupied(entry) => {
                if entry.get().attempt != attempt {
                    return;
                }
                let requester_socket = entry.get().requester_socket.clone();
                entry.remove();
                requester_socket
            }
            Entry::Vacant(_) => {
                return;
            }
        };

        _ = requester_socket.emit(
            "task_result",
            &json!({
                "task_id": task_id,
                "attempt": attempt,
                "worker_id": worker_id,
                "task_cost": task_cost,
                "result": msg.get("result").cloned().unwrap_or_else(|| json!(null)),
                "ts": now_ts(),
            }),
        );
        return;
    }

    let Some(master_id) = msg.get("master_id").and_then(|v| v.as_str()) else {
        _ = socket.emit(
            "server_error",
            &json!({
                "error": "missing_master_id",
            }),
        );
        return;
    };

    let Some(master) = SERVER_NODES.get(master_id) else {
        _ = socket.emit(
            "server_error",
            &json!({
                "error": "master_offline",
                "master_id": master_id,
            }),
        );
        return;
    };

    _ = master.socket.emit(
        "task_result",
        &json!({
            "attempt": attempt,
            "worker_id": worker_id,
            "task_cost": task_cost,
            "result": msg.get("result").cloned().unwrap_or_else(|| json!(null)),
            "ts": now_ts(),
        }),
    );
}

async fn handle_server_message(socket: SocketRef, data: Data<Value>) {
    let msg = parse_message(&data);
    let cmd = msg.get("cmd").and_then(|v| v.as_str()).unwrap_or("");

    match cmd {
        "heartbeat" => {
            if let Some(node_id) = socket.extensions.get::<String>() {
                if let Some(mut node) = SERVER_NODES.get_mut(node_id.as_str()) {
                    node.last_heartbeat = now_ts();
                }
            }
            _ = socket.emit("heartbeat_ack", &json!({ "ts": now_ts() }));
        }
        "report_load" => {
            report_load(&socket, &msg).await;
        }
        "dispatch_task" => {
            dispatch_task(&socket, &msg).await;
        }
        "task_result" => {
            forward_task_result(&socket, &msg).await;
        }
        "list_nodes" => {
            _ = socket.emit("server_nodes", &node_snapshot());
        }
        _ => {
            _ = socket.emit(
                "server_error",
                &json!({
                    "error": "unknown_cmd",
                    "cmd": cmd,
                }),
            );
        }
    }
}

pub async fn on_connect(_io: SocketIo, socket: SocketRef, Data(data): Data<Value>) {
    if !valid_server_token(&data) {
        _ = socket.disconnect();
        return;
    }

    let node_id = data
        .get("node_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if node_id.is_empty() {
        _ = socket.disconnect();
        return;
    }

    let node = ServerNode {
        node_id: node_id.clone(),
        addr: data
            .get("addr")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        role: data
            .get("role")
            .and_then(|v| v.as_str())
            .unwrap_or("worker")
            .to_string(),
        device_info: data.get("device_info").cloned().unwrap_or_else(|| json!({})),
        socket: socket.clone(),
        current_load: data
            .get("current_load")
            .and_then(|v| v.as_u64())
            .unwrap_or(0),
        max_load: data
            .get("max_load")
            .and_then(|v| v.as_u64())
            .unwrap_or(100)
            .max(1),
        last_heartbeat: now_ts(),
    };

    match SERVER_NODES.entry(node_id.clone()) {
        Entry::Occupied(mut entry) => {
            let old_socket = entry.get().socket.clone();
            dbg!(
                "replace_old_server_node",
                &node_id,
                old_socket.id,
                socket.id
            );
            _ = old_socket.disconnect();
            entry.insert(node);
        }
        Entry::Vacant(entry) => {
            entry.insert(node);
        }
    }

    socket.extensions.insert(node_id.clone());
    let socket_id = socket.id;

    _ = socket.emit(
        "register_ok",
        &json!({
            "node_id": node_id,
            "ts": now_ts(),
            "nodes": node_snapshot(),
        }),
    );

    socket.on("server_msg", handle_server_message);
    socket.on_disconnect(
        move |_socket: SocketRef, _reason: DisconnectReason| async move {
            let should_remove = SERVER_NODES
                .get(&node_id)
                .map(|node| node.socket.id == socket_id)
                .unwrap_or(false);
            if should_remove {
                SERVER_NODES.remove(&node_id);
            }
        },
    );
}

fn build_current_node_device_info() -> Value {
    json!({
        "hostname": hostname(),
        "os": std::env::consts::OS,
        "arch": std::env::consts::ARCH,
        "cpu_cores": std::thread::available_parallelism()
            .map(|num| num.get())
            .unwrap_or(1),
        "total_memory_kb": total_memory_kb(),
        "pid": std::process::id(),
        "started_at": SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or(0),
    })
}

fn hostname() -> String {
    fs::read_to_string("/etc/hostname")
        .map(|text| text.trim().to_string())
        .ok()
        .filter(|text| !text.is_empty())
        .or_else(|| std::env::var("HOSTNAME").ok())
        .unwrap_or_else(|| "unknown".to_string())
}

fn total_memory_kb() -> u64 {
    memory_field_kb("MemTotal").unwrap_or(0)
}

fn memory_usage() -> (u64, f64) {
    let total_memory_kb = memory_field_kb("MemTotal").unwrap_or(0);
    let available_memory_kb = memory_field_kb("MemAvailable").unwrap_or(0);
    let used_memory_kb = total_memory_kb.saturating_sub(available_memory_kb);
    let memory_used_ratio = if total_memory_kb > 0 {
        used_memory_kb as f64 / total_memory_kb as f64 * 100.0
    } else {
        0.0
    };

    (used_memory_kb, memory_used_ratio)
}

fn memory_field_kb(field_name: &str) -> Option<u64> {
    let Ok(text) = fs::read_to_string("/proc/meminfo") else {
        return None;
    };

    text.lines()
        .find_map(|line| {
            let value = line.strip_prefix(&format!("{field_name}:"))?.trim();
            value
                .split_whitespace()
                .next()
                .and_then(|num| num.parse::<u64>().ok())
        })
}
