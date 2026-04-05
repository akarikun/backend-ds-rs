use crate::commons::utils::CONFIG as config;
use crate::commons::utils::{self};
use crate::distributed;
use crate::worker_task;
use bytes::Bytes;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use salvo::catcher::Catcher;
use salvo::prelude::*;
use serde_json::{Value, json};
use socketioxide::SocketIo;
use socketioxide::extract::{Data, SocketRef};
use socketioxide::socket::DisconnectReason;
use tokio::time::{Duration, interval};
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;

lazy_static::lazy_static! {
    static ref CLIENT_MAP: DashMap<String, SocketRef> = DashMap::new();
}

pub fn get_client_count() -> usize {
    CLIENT_MAP.len()
}

fn valid_user(userid: &str, validuser: &str) -> bool {
    match utils::decrypt_cryptojs_aes(validuser, &utils::get_aes_passphrase()) {
        Ok(u) => &u == userid,
        Err(_) => false,
    }
}

fn decode_msgpack(data: &Data<Bytes>) -> Result<Value, rmp_serde::decode::Error> {
    rmp_serde::from_slice(data.0.as_ref())
}

fn encode_msgpack(value: &Value) -> Option<Bytes> {
    rmp_serde::to_vec(value).ok().map(Bytes::from)
}

async fn on_connect(_io: SocketIo, socket: SocketRef, Data(data): Data<Value>) {
    let mut is_create_user = false;
    let userid = data
        .get("userid")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let validuser = data.get("validuser").and_then(|v| v.as_str()).unwrap_or("");

    if let (Some(validcmd), Some(cmd)) = (data.get("validcmd"), data.get("cmd")) {
        if let (Value::String(validcmd), Value::String(cmd)) = (validcmd, cmd) {
            if !valid_user(validcmd, cmd) {
                _ = socket.disconnect();
                return;
            }
            if cmd == "create_user" {
                is_create_user = true;
            }
        } else {
            _ = socket.disconnect();
            return;
        }
    }

    if !is_create_user && (userid.is_empty() || !valid_user(&userid, validuser)) {
        _ = socket.disconnect();
        return;
    }

    if !is_create_user {
        // 原子检查 + 写入，避免并发下同一个 userid 同时登录。
        match CLIENT_MAP.entry(userid.clone()) {
            Entry::Occupied(_) => {
                dbg!("user_online", &userid);
                _ = socket.disconnect();
                return;
            }
            Entry::Vacant(entry) => {
                entry.insert(socket.clone());
            }
        }
    }

    // 鉴权通过后，在这里挂玩家消息事件处理器
    socket.on(
        "message",
        |socket: SocketRef, data: Data<Bytes>| async move {
            match decode_msgpack(&data) {
                Ok(data) => {
                    // dbg!("message", socket.id, &data);
                    if let Some((event, resp)) = worker_task::run_client_task(data.clone()).await {
                        if let Some(payload) = encode_msgpack(&resp) {
                            _ = socket.emit(event.as_str(), &payload);
                        }
                    } else {
                        dbg!("unknown_message", socket.id, &data);
                    }
                }
                Err(err) => {
                    dbg!("invalid_message", socket.id, err.to_string());
                }
            }
        },
    );

    socket.on_disconnect({
        move |_socket: SocketRef, _reason: DisconnectReason| async move {
            if !is_create_user {
                CLIENT_MAP.remove(&userid);
            }
        }
    });
}

fn dashboard_state() -> Value {
    let distributed = config.distributed.clone();
    let db = config.db.clone();
    let redis = config.redis.clone();
    json!({
        "config": {
            "host": config.host,
            "client_ns": config.client_ns,
            "dashboard_ns": config.dashboard_ns,
            "aes_passphrase": "***",
            "db": {
                "kind": db.kind,
                "mongodb_uri": mask_secret_url(db.mongodb_uri.as_str()),
                "mongodb_db": db.mongodb_db,
                "sql_url": mask_secret_url(db.sql_url.as_str()),
            },
            "redis": {
                "enabled": redis.enabled,
                "redis_url": mask_secret_url(redis.redis_url.as_str()),
                "key_prefix": redis.key_prefix,
                "default_ttl_secs": redis.default_ttl_secs,
            },
            "distributed": {
                "server_ns": distributed.server_ns,
                "server_token": "***",
                "node_id": distributed.node_id,
                "node_role": distributed.node_role,
                "node_addr": distributed.node_addr,
                "master_addr": distributed.master_addr,
                "max_load": distributed.max_load,
                "heartbeat_interval_secs": distributed.heartbeat_interval_secs,
                "node_timeout_secs": distributed.node_timeout_secs,
            }
        },
        "current_node_device": distributed::current_node_device_info(),
        "player_count": get_client_count(),
        "distributed": distributed::dashboard_snapshot(),
    })
}

fn mask_secret_url(raw_url: &str) -> String {
    let Ok(mut parsed) = url::Url::parse(raw_url) else {
        return raw_url.to_string();
    };

    if parsed.password().is_some() {
        let _ = parsed.set_password(Some("***"));
    }

    parsed.to_string()
}

async fn dashboard_connect(_io: SocketIo, socket: SocketRef, Data(_data): Data<Value>) {
    _ = socket.emit("dashboard", &dashboard_state());

    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(1));
        loop {
            ticker.tick().await;
            if socket.emit("dashboard", &dashboard_state()).is_err() {
                break;
            }
        }
    });
}

pub fn config_router() -> Router {
    let (layer, io) = SocketIo::new_layer();
    let layer = ServiceBuilder::new()
        .layer(CorsLayer::permissive())
        .layer(layer);

    io.ns(config.client_ns.as_str(), on_connect);
    io.ns(
        config.distributed.server_ns.as_str(),
        distributed::on_connect,
    );
    io.ns(config.dashboard_ns.as_str(), dashboard_connect);
    let layer = layer.compat();
    Router::new()
        .push(Router::with_path(config.socket_path.as_str()).hoop(layer).goal(hello))
        .push(Router::with_path("/api/dashboard").get(dashboard_meta))
}

pub fn config_catcher() -> Catcher {
    Catcher::default().hoop(rewrite_405_to_404)
}

#[handler]
async fn hello() -> &'static str {
    ""
}

#[handler]
async fn dashboard_meta(res: &mut Response) {
    res.render(Json(json!({
        "dashboard_ns": config.dashboard_ns,
        "socket_path": config.socket_path,
        "client_ns": config.client_ns,
    })));
}

// 将 405 Method Not Allowed 转换为 404 Not Found，并渲染统一的 404 页面。
#[handler]
async fn rewrite_405_to_404(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
    ctrl: &mut FlowCtrl,
) {
    if res.status_code == Some(StatusCode::METHOD_NOT_ALLOWED) {
        res.status_code(StatusCode::NOT_FOUND);
    }

    if res.status_code == Some(StatusCode::NOT_FOUND) {
        render_not_found(req, res);
        ctrl.skip_rest();
        return;
    }

    ctrl.call_next(req, depot, res).await;
}

// 根据请求的 Accept 头和 URL 路径，返回 JSON 格式的 404 错误信息，或者渲染一个简单的 HTML 404 页面。
fn render_not_found(req: &Request, res: &mut Response) {
    let accept = req
        .headers()
        .get("accept")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let path = req.uri().path();

    if path.starts_with("/api/") || accept.contains("application/json") {
        res.render(Json(json!({
            "code": 404,
            "message": "resource not found",
            "path": path,
        })));
        return;
    }

    res.render(Text::Html(
        r#"<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>404 Not Found</title>
    <style>
        body {
            margin: 0;
            min-height: 100vh;
            display: grid;
            place-items: center;
            background: #f6f7fb;
            color: #1f2937;
            font-family: sans-serif;
        }
        main {
            padding: 32px;
            text-align: center;
        }
        h1 {
            margin: 0 0 12px;
            font-size: 64px;
        }
        p {
            margin: 0;
            font-size: 16px;
        }
    </style>
</head>
<body>
    <main>
        <h1>404</h1>
        <p>页面不存在</p>
    </main>
</body>
</html>"#,
    ));
}
