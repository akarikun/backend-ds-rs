use std::{fs, io::Write};

use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, Serialize, Debug)]
#[serde(default)]
pub struct Config {
    // 当前服务监听地址，Salvo HTTP 和 socket.io 都挂在这个地址上。
    pub host: String,
    // 玩家客户端连接的 socket.io namespace。
    pub client_ns: String,
    // 后台管理面板连接的 socket.io namespace。
    pub dashboard_ns: String,
    // 玩家身份校验用的 AES 密钥短语，需和客户端加密逻辑保持一致。
    pub aes_passphrase: String,
    // 数据库配置：所有 master/worker 节点都可以直连同一台 DB。
    pub db: DbConfig,
    // Redis 配置：当前先用来做简单 key-value JSON 缓存。
    pub redis: RedisConfig,
    // 分布式后台节点相关配置。
    pub distributed: DistributedConfig,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
#[serde(default)]
pub struct DbConfig {
    // 数据库类型：mongodb 或 postgresql。
    pub kind: String,
    // MongoDB 连接串。
    pub mongodb_uri: String,
    // MongoDB 数据库名。
    pub mongodb_db: String,
    // PostgreSQL 连接串。
    pub postgresql_url: String,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
#[serde(default)]
pub struct DistributedConfig {
    // 服务端节点内部通信的 socket.io namespace，建议改成不容易猜到的路径。
    pub server_ns: String,
    // 内部节点握手令牌，master 和 worker 必须一致，不匹配会被断开。
    pub server_token: String,
    // 当前节点唯一 ID，同一个集群里不能重复。
    pub node_id: String,
    // 当前节点角色：master 表示调度节点，worker 表示工作节点。
    pub node_role: String,
    // 当前节点自己的对外地址，主要用于注册展示和以后 master 主动回连 worker。
    pub node_addr: String,
    // master 节点地址；worker 会主动连接这个地址，master 自己可以填自身地址。
    pub master_addr: String,
    // 当前节点最大可承载任务量，worker 上报负载和 master 调度选节点时会用到。
    pub max_load: u64,
    // worker 向 master 发送心跳和负载上报的间隔秒数。
    pub heartbeat_interval_secs: u64,
    // master 判定节点失联的超时时间秒数，超过这个时间没心跳就可认为节点不可用。
    pub node_timeout_secs: u64,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
#[serde(default)]
pub struct RedisConfig {
    // 是否启用 Redis 缓存；先默认关闭，避免本地没启动 Redis 时影响服务启动。
    pub enabled: bool,
    // Redis 连接地址。
    pub redis_url: String,
    // 缓存 key 前缀，方便区分环境/业务。
    pub key_prefix: String,
    // 默认缓存 TTL 秒数。
    pub default_ttl_secs: u64,
}

impl Default for DistributedConfig {
    fn default() -> Self {
        Self {
            server_ns: "/server".to_string(),
            server_token: "V0.0.1-SERVER".to_string(),
            node_id: "node-1".to_string(),
            node_role: "master".to_string(),
            node_addr: "127.0.0.1:8082".to_string(),
            master_addr: "127.0.0.1:8082".to_string(),
            max_load: 100,
            heartbeat_interval_secs: 5,
            node_timeout_secs: 15,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: "127.0.0.1:8082".to_string(),
            client_ns: "/ws".to_string(),
            dashboard_ns: "/api/dashboard".to_string(),
            aes_passphrase: "V0.0.1-A265".to_string(),
            db: DbConfig::default(),
            redis: RedisConfig::default(),
            distributed: DistributedConfig::default(),
        }
    }
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            kind: "mongodb".to_string(),
            mongodb_uri: "mongodb://admin:123456@127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=backend_ds".to_string(),
            mongodb_db: "backend_ds".to_string(),
            postgresql_url: "postgres://postgres:123456@localhost:5432/backend_ds"
                .to_string(),
        }
    }
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            redis_url: "redis://127.0.0.1:6379/".to_string(),
            key_prefix: "backend_ds:".to_string(),
            default_ttl_secs: 300,
        }
    }
}

impl Config {
    pub fn load() -> std::io::Result<Self> {
        let path = "config.json";
        let err_msg = &format!("<{path}>文件不存在或配置异常");
        if !fs::exists(path).expect(err_msg) {
            let mut file = fs::File::create(path).expect(err_msg);
            let json_str = serde_json::to_string_pretty(&Self::default())?;
            file.write_all(json_str.as_bytes())?;
            panic!("请配置config.json相关参数后运行");
        }

        let text = fs::read_to_string(path).expect(err_msg);
        let cfg: Self = serde_json::from_str(&text).expect(err_msg);
        Ok(cfg)
    }
}
