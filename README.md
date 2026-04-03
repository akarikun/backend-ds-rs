### 一个简单的分布式后台服务

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