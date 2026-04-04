### 一个简单的分布式后台服务

##### 使用`Salvo` + `Socket.IO`

- 推荐部署顺序：运行当前程序会在当前目录添加 `config.json` 配置，然后退出程序；如果是 PostgreSQL 空库，先调用一次 `cargo test test_init_postgresql_schema --lib -- --nocapture` 初始化表结构（非必需操作，主要用于测试），更新 `config.json` 配置后，再运行当前程序。
- 当前架构边界：建议按单 master + 多 worker 使用，暂时不要直接扩成多 master，避免 addon 同步和调度一致性变复杂。
- `task_idempotency` 不是所有任务必需的数据表/集合，只有请求里显式传了 `task_id` 才会用它做幂等和结果回放；如果暂时不需要任务幂等，可以先不创建这张表/集合。启用后它目前不会自动清理，长期运行会持续增长，后面可以按保留天数加一个定期清理策略。
- addon JS 同步规则：worker 连接 master 时会同步 master 的脚本；如果 master 上更新了 `addons/*.js`，需要 master 重启或 worker 重连后 worker 才会拿到新版本。
- 正式部署前建议做一次完整联调：master、worker、PostgreSQL/MongoDB、Redis、玩家创建/查询、发奖、邮件、幂等重试、dashboard 页面展示都跑一遍。


### config.json配置 具体可参考config.rs备注
```
{
  "host": "0.0.0.0:8082",
  "client_ns": "/ws",
  "dashboard_ns": "/api/dashboard",
  "aes_passphrase": "V0.0.1-A265",
  "db": {
    "kind": "postgresql",
    "mongodb_uri": "mongodb://admin:123456@127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=backend_ds",
    "mongodb_db": "backend_ds",
    "sql_url": "postgresql://postgres:postgres@127.0.0.1:5432/backend_ds"
  },
  "redis":{
    "enabled": false,
    "redis_url": "redis://127.0.0.1:6379/",
    "key_prefix": "backend_ds:",
    "default_ttl_secs": 300
  },
  "distributed": {
    "server_ns": "/server",
    "server_token": "V0.0.1-SERVER",
    "node_id": "master-1",
    "node_role": "master",
    "node_addr": "192.168.1.1:8082",
    "master_addr": "192.168.1.1:8082",
    "max_load": 100,
    "heartbeat_interval_secs": 5,
    "node_timeout_secs": 15
  }
}
```

`db.kind` 和 `db.sql_url` 说明：

- `postgresql`: `sql_url` 形如 `postgresql://postgres:postgres@127.0.0.1:5432/backend_ds`
- `mysql`: `sql_url` 形如 `mysql://root:123456@127.0.0.1:3306/backend_ds`
- `sqlite`: `sql_url` 形如 `sqlite://./backend_ds.db`
- `mongodb`: 使用 `mongodb_uri + mongodb_db`


### 分布式任务流程

- Worker 启动后会主动连接 master 的 `distributed.server_ns`，注册自身 `node_id/node_role/node_addr/max_load`。
- Worker 会按 `heartbeat_interval_secs` 周期性向 master 发送 `heartbeat`、`report_load`、`list_nodes`。
- Master 收到 `dispatch_task` 后，优先选择当前负载最低且未超过 `max_load` 的 worker 下发 `task`；如果没有可用 worker，或者任务类型需要 master 本地执行，则直接本机执行。
- Worker 执行完成后向 master 发 `server_msg { "cmd": "task_result", ... }`，master 按 `task_id/attempt` 找回原始请求方并转发 `task_result`。
- Master 侧会对远端任务做超时监控，超时后按 `max_retries` 自动重派；重试耗尽返回 `task_timeout`。

### test_init_postgresql_schema（非必需操作，主要用于测试）
##### 玩家数据表

注意：如果 PostgreSQL 是空库，启动服务前必须先跑一次 `test_init_postgresql_schema`，这个测试方法内部会调用 `init_postgresql_schema()` 初始化表结构，否则业务 SQL 会因为表不存在而失败。

- `player_profiles`: 玩家基础档案，保存昵称、等级、经验、头像、创建/更新时间。
- `player_wallets`: 玩家货币账户，保存金币、钻石、体力。
- `player_items`: 玩家背包道具，按 `userid + item_id` 唯一约束聚合数量。
- `player_mails`: 玩家邮件，支持附件 JSON、已读/未读状态、发信时间。
- `task_idempotency`: 分布式任务幂等表/集合，按 `task_id` 记录任务执行状态、attempt 和最终结果。

部署完成后可以在addon-test.html测试
http://0.0.0.0:8082/addon-test.html

![1.png](https://raw.githubusercontent.com/akarikun/backend-ds-rs/refs/heads/main/1.png)
![2.png](https://raw.githubusercontent.com/akarikun/backend-ds-rs/refs/heads/main/2.png)

### 分布式任务协议

向 `/server` namespace 发送：

```json
{
  "cmd": "dispatch_task",
  "task_id": "mail-1001",
  "attempt": 1,
  "timeout_secs": 5,
  "max_retries": 2,
  "task_cost": 10,
  "task": {
    "type": "send_mail",
    "userid": "u1",
    "title": "hello",
    "content": "world"
  }
}
```

会先收到派发确认：

```json
{
  "ok": true,
  "task_id": "mail-1001",
  "attempt": 1,
  "worker_id": "worker-1",
  "task_cost": 10,
  "timeout_secs": 5,
  "retries_left": 2,
  "ts": 1710000000
}
```

任务完成后收到结果：

```json
{
  "task_id": "mail-1001",
  "attempt": 1,
  "worker_id": "worker-1",
  "task_cost": 10,
  "result": {
    "ok": true
  },
  "ts": 1710000001
}
```

### 任务幂等和重试规则

- `task_id` 相同表示同一笔业务任务。建议非幂等写操作都显式传 `task_id`，例如发奖、发邮件、扣道具。
- 如果请求里不传 `task_id`，worker 会直接执行任务，不会访问 `task_idempotency`，因此没有这张表/集合也能运行普通任务。
- 如果请求里传了 `task_id`，但数据库里没有 `task_idempotency` 表/集合，这次任务会直接返回数据库错误。
- Worker 真正执行业务前会先抢占 `task_idempotency` 记录；抢到锁才执行，已完成则直接回放旧结果。
- 如果同一 `task_id` 正在执行中，后来的重复请求会最多等待 10 秒轮询旧结果；等到则直接回放，等不到返回 `task_running_wait_timeout`。
- 如果 worker 执行中崩溃，`running` 幂等记录超过 300 秒没更新后，允许新 attempt 接管这条 `task_id` 继续执行。
- 旧 attempt 的迟到结果会被 master 丢弃，只释放 worker 负载，不会覆盖当前 attempt 的返回。
- 常见错误码：`duplicate_task_id`、`task_timeout`、`task_retry_no_available_worker`、`task_retry_emit_failed`、`task_running_wait_timeout`、`task_idempotency_state_lost`、`finish_task_idempotency_failed`。


### 任务实现优先级：JS 覆盖 Rust

只需要在Master中部署 `addons/*.js`， Worker同步时会将JS代码同步并加载到内存中

同一个任务类型会优先执行 `addons/*.js` 里的 JS handler；如果 JS 没有实现这个任务，才会回退到 Rust 内置 handler。

也就是说，如果你后面在 Rust 里实现了 `create_player`，但某些场景想临时调整逻辑，只要在 addon JS 里写一个同名的 `$register_task("create_player", ...)`，就能覆盖 Rust 版本；删除这个 JS handler 后，又会自动回退到 Rust 版本。

Rust 任务注册格式在 `src/worker_task.rs` 的 `TASK_METHODS` 里，例如：

```rust
// map.insert("create_player", create_player_handler as TaskHandler);
```

Rust 版 `create_player_handler` 的 demo 也已经注释写在 `src/worker_task.rs` 末尾，可以直接按那个格式扩展自己的任务。

### Addon DB 用法

```js
// PG 这类 SQL：db.db_query(...) / db.db_transaction(...)
// Mongo 这类文档库：db.doc_query(...)

$register_task("get_player", async (db, data) => {
  let rows = await db.db_query(
    "select * from player_profiles where userid = ?",
    [data.userid || ""]
  );
  return rows[0] || null;
});

// 单条 SQL 版
$register_task("create_player", async (db, data) => {
  let rows = await db.db_query(
    `insert into player_profiles (userid, nickname)
     values (?, ?)
     on conflict (userid)
     do update set nickname = excluded.nickname, updated_at = now()
     returning *`,
    [data.userid || "", data.nickname || "player"]
  );
  return rows[0] || null;
});

// 事务版，推荐多表写入时用这种；和上面的单条 SQL 版二选一，不要重复注册同名任务。
$register_task("create_player", async (db, data) => {
  let result = await db.db_transaction([
    {
      query: `insert into player_profiles (userid, nickname)
              values (?, ?)
              on conflict (userid)
              do update set nickname = excluded.nickname, updated_at = now()
              returning *`,
      params: [data.userid || "", data.nickname || "player"],
    },
    {
      query: `insert into player_wallets (userid, gold, diamond, stamina)
              values (?, ?, ?, ?)
              on conflict (userid)
              do update set updated_at = now()
              returning *`,
      params: [data.userid || "", 0, 0, 100],
    },
  ]);
  if (result.ok === false) {
    return result;
  }
  return {
    profile: (result[0] || [])[0] || null,
    wallet: (result[1] || [])[0] || null,
  };
});

// Mongo 示例，没有特意测试，目前以 PG 为主。
$register_task("get_player_profile", async (db, data) => {
  let rows = await db.doc_query("player_profiles", {
    op: "find_one",
    filter: {
      userid: data.userid || "",
    },
  });
  return rows[0] || null;
});

$register_task("create_player_profile", async (db, data) => {
  let inserted = await db.doc_query("player_profiles", {
    op: "insert_one",
    document: {
      userid: data.userid || "",
      nickname: data.nickname || "player",
      level: 1,
      exp: 0,
      avatar_id: 0,
      created_at: Date.now(),
      updated_at: Date.now(),
    },
  });
  return inserted[0] || null;
});

$register_task("rename_player_profile", async (db, data) => {
  let updated = await db.doc_query("player_profiles", {
    op: "update_one",
    filter: {
      userid: data.userid || "",
    },
    update: {
      $set: {
        nickname: data.nickname || "player",
        updated_at: Date.now(),
      },
    },
    upsert: true,
  });
  return updated[0] || null;
});
```

### Addon Redis 缓存用法

Rust 侧会把 `$cache.get/$cache.set/$cache.del` 注入 addon JS。当前 `addons/connector.js` 先对 `get_player` 做了一个简单的玩家快照缓存，写操作后会强制回源并刷新缓存。

```js
let player = await $cache.get(`player:${userid}`);
await $cache.set(`player:${userid}`, player, 300);
await $cache.del(`player:${userid}`);
```
