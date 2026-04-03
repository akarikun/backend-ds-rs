### 一个简单的分布式后台服务

### 玩家数据表

- `player_profiles`: 玩家基础档案，保存昵称、等级、经验、头像、创建/更新时间。
- `player_wallets`: 玩家货币账户，保存金币、钻石、体力。
- `player_items`: 玩家背包道具，按 `userid + item_id` 唯一约束聚合数量。
- `player_mails`: 玩家邮件，支持附件 JSON、已读/未读状态、发信时间。

### Addon DB 用法

```js
query("get_player", async (db, data) => {
  let rows = await db.query(
    "select * from player_profiles where userid = ?",
    [data.userid || ""]
  );
  return rows[0] || null;
});

query("create_player", async (db, data) => {
  let rows = await db.query(
    `insert into player_profiles (userid, nickname)
     values (?, ?)
     on conflict (userid)
     do update set nickname = excluded.nickname, updated_at = now()
     returning *`,
    [data.userid || "", data.nickname || "player"]
  );
  return rows[0] || null;
});
```

```
# Master
"node_id": "master-1",
"node_role": "master",

# Worker
"node_id": "worker-1",
"node_role": "worker",

{
  "host": "0.0.0.0:8082",
  "client_ns": "/ws",
  "dashboard_ns": "/api/dashboard",
  "aes_passphrase": "V0.0.1-A265",
  "db": {
    "kind": "mongodb",
    "kind_desc": "数据库类型，支持 mongodb 或 postgresql",
    "mongodb_uri": "mongodb://admin:123456@127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=backend_ds",
    "mongodb_db": "backend_ds",
    "postgresql_url": "postgresql://postgres:postgres@127.0.0.1:5432/backend_ds"
  },
  "distributed": {
    "server_ns": "/server",
    "server_token": "V0.0.1-SERVER",
    "node_id": "master-1",
    "node_role": "master",
    "node_addr": "127.0.0.1:8082",
    "master_addr": "127.0.0.1:8082",
    "max_load": 100,
    "heartbeat_interval_secs": 5,
    "node_timeout_secs": 15
  }
}
```
